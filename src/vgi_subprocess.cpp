#include "vgi_subprocess.hpp"

#include <cerrno>
#include <cstdlib>
#include <cstring>
#include <sys/select.h>

#include "duckdb/common/exception.hpp"

namespace duckdb {
namespace vgi {

// Pipe implementation
Pipe::Pipe() {
	int fds[2];
	if (pipe(fds) != 0) {
		throw IOException("Failed to create pipe");
	}
	read_fd = fds[0];
	write_fd = fds[1];
}

Pipe::~Pipe() {
	CloseRead();
	CloseWrite();
}

void Pipe::CloseRead() {
	if (read_fd >= 0) {
		close(read_fd);
		read_fd = -1;
	}
}

void Pipe::CloseWrite() {
	if (write_fd >= 0) {
		close(write_fd);
		write_fd = -1;
	}
}

// SubProcess implementation
SubProcess::SubProcess(const std::string &command, bool stderr_passthrough) {
	// Also check environment variable for backwards compatibility
	if (!stderr_passthrough) {
		const char *passthrough_env = std::getenv("VGI_WORKER_STDERR_PASSTHROUGH");
		stderr_passthrough = passthrough_env && std::string(passthrough_env) == "1";
	}

	Pipe stdin_pipe;
	Pipe stdout_pipe;
	Pipe stderr_pipe; // Only used if not passthrough

	pid_ = fork();
	if (pid_ < 0) {
		throw IOException("Failed to fork process");
	}

	if (pid_ == 0) {
		// Child process
		// Redirect stdin
		dup2(stdin_pipe.read_fd, STDIN_FILENO);
		stdin_pipe.CloseRead();
		stdin_pipe.CloseWrite();

		// Redirect stdout
		dup2(stdout_pipe.write_fd, STDOUT_FILENO);
		stdout_pipe.CloseRead();
		stdout_pipe.CloseWrite();

		// Redirect stderr (only if not passthrough)
		if (!stderr_passthrough) {
			dup2(stderr_pipe.write_fd, STDERR_FILENO);
			stderr_pipe.CloseRead();
			stderr_pipe.CloseWrite();
		} else {
			// Enable IPC debug output in the worker when debugging
			setenv("VGI_IPC_DEBUG", "1", 1);
		}
		// If passthrough, stderr remains connected to parent's stderr

		// Execute command via shell
		execl("/bin/sh", "sh", "-c", command.c_str(), nullptr);
		_exit(127); // exec failed
	}

	// Parent process
	stdin_fd_ = stdin_pipe.write_fd;
	stdin_pipe.write_fd = -1; // Transfer ownership

	stdout_fd_ = stdout_pipe.read_fd;
	stdout_pipe.read_fd = -1; // Transfer ownership

	if (!stderr_passthrough) {
		stderr_fd_ = stderr_pipe.read_fd;
		stderr_pipe.read_fd = -1; // Transfer ownership
	} else {
		stderr_fd_ = -1; // No stderr pipe when passthrough
	}

	// Close unused ends
	stdin_pipe.CloseRead();
	stdout_pipe.CloseWrite();
	if (!stderr_passthrough) {
		stderr_pipe.CloseWrite();
	}
}

SubProcess::~SubProcess() {
	if (stdin_fd_ >= 0) {
		close(stdin_fd_);
	}
	if (stdout_fd_ >= 0) {
		close(stdout_fd_);
	}
	if (stderr_fd_ >= 0) {
		close(stderr_fd_);
	}
	if (pid_ > 0) {
		// First check if the process has already exited (non-blocking)
		int status;
		pid_t result = waitpid(pid_, &status, WNOHANG);
		if (result == 0) {
			// Process still running, send SIGTERM and wait
			kill(pid_, SIGTERM);
			waitpid(pid_, &status, 0);
		}
		// If result > 0, process already exited, nothing more to do
		// If result < 0, error (process doesn't exist), nothing to do
	}
}

void SubProcess::CloseStdin() {
	if (stdin_fd_ >= 0) {
		close(stdin_fd_);
		stdin_fd_ = -1;
	}
}

void SubProcess::CloseStderr() {
	if (stderr_fd_ >= 0) {
		close(stderr_fd_);
		stderr_fd_ = -1;
	}
}

int SubProcess::Wait(bool *exited_normally) {
	if (pid_ <= 0) {
		if (exited_normally) {
			*exited_normally = true;
		}
		return 0;
	}

	int status;
	pid_t result;
	while (true) {
		result = waitpid(pid_, &status, 0);
		if (result >= 0) {
			break;
		}
		if (errno == EINTR) {
			continue; // Interrupted by signal, retry
		}
		// Error waiting for process
		if (exited_normally) {
			*exited_normally = false;
		}
		return -1;
	}

	pid_ = -1; // Mark as waited

	if (WIFEXITED(status)) {
		if (exited_normally) {
			*exited_normally = true;
		}
		return WEXITSTATUS(status);
	} else if (WIFSIGNALED(status)) {
		if (exited_normally) {
			*exited_normally = false;
		}
		return WTERMSIG(status);
	}

	if (exited_normally) {
		*exited_normally = false;
	}
	return -1;
}

// WriteAll implementation
void WriteAll(int fd, const uint8_t *data, size_t len) {
	size_t written = 0;
	while (written < len) {
		ssize_t result = write(fd, data + written, len - written);
		if (result < 0) {
			if (errno == EINTR) {
				continue; // Interrupted by signal, retry
			}
			throw IOException("Failed to write to pipe: %s", strerror(errno));
		}
		written += result;
	}
}

// WaitForReadable implementation - wait for fd to be readable with timeout
void WaitForReadable(int fd, int timeout_seconds) {
	fd_set read_fds;
	struct timeval timeout;

	FD_ZERO(&read_fds);
	FD_SET(fd, &read_fds);

	timeout.tv_sec = timeout_seconds;
	timeout.tv_usec = 0;

	int result = select(fd + 1, &read_fds, nullptr, nullptr, &timeout);

	if (result < 0) {
		if (errno == EINTR) {
			// Interrupted by signal, treat as timeout for simplicity
			throw IOException("VGI catalog operation interrupted");
		}
		throw IOException("VGI catalog operation failed: select error: %s", strerror(errno));
	}

	if (result == 0) {
		// Timeout
		throw IOException("VGI catalog operation timed out after %d seconds", timeout_seconds);
	}

	// fd is readable, return successfully
}

} // namespace vgi
} // namespace duckdb
