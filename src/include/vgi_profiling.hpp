// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#pragma once

#include <chrono>
#include <cstdlib>
#include <iomanip>
#include <iostream>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

namespace duckdb {
namespace vgi {

// Check if profiling is enabled via VGI_PROFILE environment variable.
// Result is cached after first call.
inline bool VgiProfilingEnabled() {
	static bool initialized = false;
	static bool enabled = false;
	if (!initialized) {
		const char *env = std::getenv("VGI_PROFILE");
		enabled = env && (std::string(env) == "1" || std::string(env) == "true");
		initialized = true;
	}
	return enabled;
}

// Thread-safe profiling stats collector
class ProfileStats {
public:
	struct TimingEntry {
		std::string name;
		double duration_ms;
		int64_t count;       // For batch operations, number of rows/items
		std::string detail;  // Optional detail string
	};

	// Use heap-allocated singleton that is never destroyed (survives atexit)
	static ProfileStats &Instance() {
		static ProfileStats *instance = new ProfileStats();
		return *instance;
	}

	void Record(const std::string &name, double duration_ms, int64_t count = 0, const std::string &detail = "") {
		std::lock_guard<std::mutex> lock(mutex_);
		entries_.push_back({name, duration_ms, count, detail});
	}

	void Clear() {
		std::lock_guard<std::mutex> lock(mutex_);
		entries_.clear();
	}

	// Print a summary grouped by operation name (internal implementation)
	void PrintSummaryImpl() {
		if (entries_.empty()) {
			return;
		}

		// Group by name
		std::unordered_map<std::string, std::vector<TimingEntry>> grouped;
		for (const auto &entry : entries_) {
			grouped[entry.name].push_back(entry);
		}

		std::cerr << "\n[VGI PROFILE SUMMARY]\n";
		std::cerr << std::string(80, '=') << "\n";

		double total_ms = 0;
		for (const auto &pair : grouped) {
			double sum = 0;
			int64_t total_count = 0;
			for (const auto &e : pair.second) {
				sum += e.duration_ms;
				total_count += e.count;
			}
			total_ms += sum;

			std::cerr << std::left << std::setw(40) << pair.first << " | calls: " << std::setw(6) << pair.second.size()
			          << " | total: " << std::setw(10) << std::fixed << std::setprecision(3) << sum << " ms";
			if (total_count > 0) {
				std::cerr << " | rows: " << total_count;
			}
			if (pair.second.size() > 1) {
				std::cerr << " | avg: " << std::setprecision(3) << (sum / static_cast<double>(pair.second.size())) << " ms";
			}
			std::cerr << "\n";
		}

		std::cerr << std::string(80, '-') << "\n";
		std::cerr << "Total time: " << std::fixed << std::setprecision(3) << total_ms << " ms\n";
		std::cerr << std::string(80, '=') << "\n\n";
	}

	// Print a summary grouped by operation name (thread-safe)
	void PrintSummary() {
		std::lock_guard<std::mutex> lock(mutex_);
		PrintSummaryImpl();
	}

	// Print summary without locking (for use in atexit where threads may be gone)
	void PrintSummaryNoLock() {
		PrintSummaryImpl();
	}

	// Print all entries in order (detailed view)
	void PrintDetailed() {
		std::lock_guard<std::mutex> lock(mutex_);
		if (entries_.empty()) {
			return;
		}

		std::cerr << "\n[VGI PROFILE DETAILED]\n";
		for (const auto &e : entries_) {
			std::cerr << "[VGI PROFILE] " << std::left << std::setw(40) << e.name << " " << std::fixed
			          << std::setprecision(3) << std::setw(10) << e.duration_ms << " ms";
			if (e.count > 0) {
				std::cerr << " (" << e.count << " rows)";
			}
			if (!e.detail.empty()) {
				std::cerr << " [" << e.detail << "]";
			}
			std::cerr << "\n";
		}
	}

private:
	std::mutex mutex_;
	std::vector<TimingEntry> entries_;
};

// RAII timer that records to ProfileStats on destruction
class ScopedTimer {
public:
	explicit ScopedTimer(const std::string &name, int64_t count = 0, const std::string &detail = "")
	    : name_(name), count_(count), detail_(detail), enabled_(VgiProfilingEnabled()) {
		if (enabled_) {
			start_ = std::chrono::high_resolution_clock::now();
		}
	}

	~ScopedTimer() {
		if (enabled_) {
			auto end = std::chrono::high_resolution_clock::now();
			double duration_ms = std::chrono::duration<double, std::milli>(end - start_).count();
			ProfileStats::Instance().Record(name_, duration_ms, count_, detail_);
		}
	}

	// Update count after construction (useful when count is known later)
	void SetCount(int64_t count) {
		count_ = count;
	}

	void SetDetail(const std::string &detail) {
		detail_ = detail;
	}

private:
	std::string name_;
	int64_t count_;
	std::string detail_;
	bool enabled_;
	std::chrono::high_resolution_clock::time_point start_;
};

// Macro for easy profiling - creates a scoped timer
#define VGI_PROFILE_SCOPE(name) ::duckdb::vgi::ScopedTimer _vgi_timer_##__LINE__(name)
#define VGI_PROFILE_SCOPE_ROWS(name, rows) ::duckdb::vgi::ScopedTimer _vgi_timer_##__LINE__(name, rows)
#define VGI_PROFILE_SCOPE_DETAIL(name, detail) ::duckdb::vgi::ScopedTimer _vgi_timer_##__LINE__(name, 0, detail)

// Manual timing helper (for when RAII doesn't work well)
class ManualTimer {
public:
	ManualTimer() : enabled_(VgiProfilingEnabled()) {
	}

	void Start() {
		if (enabled_) {
			start_ = std::chrono::high_resolution_clock::now();
		}
	}

	double StopMs() {
		if (enabled_) {
			auto end = std::chrono::high_resolution_clock::now();
			return std::chrono::duration<double, std::milli>(end - start_).count();
		}
		return 0;
	}

	void Record(const std::string &name, int64_t count = 0, const std::string &detail = "") {
		if (enabled_) {
			ProfileStats::Instance().Record(name, StopMs(), count, detail);
		}
	}

private:
	bool enabled_;
	std::chrono::high_resolution_clock::time_point start_;
};

// Print profile summary on demand
inline void VgiProfilePrintSummary() {
	if (VgiProfilingEnabled()) {
		ProfileStats::Instance().PrintSummary();
	}
}

// Print detailed profile on demand
inline void VgiProfilePrintDetailed() {
	if (VgiProfilingEnabled()) {
		ProfileStats::Instance().PrintDetailed();
	}
}

// Clear profile stats
inline void VgiProfileClear() {
	ProfileStats::Instance().Clear();
}

// Register atexit handler to print summary when process exits
// Call this once during extension initialization
inline void VgiProfileRegisterAtExit() {
	if (VgiProfilingEnabled()) {
		static bool registered = false;
		if (!registered) {
			std::atexit([]() {
				// Use a simple print without locking since we're in atexit
				// and other threads may have been terminated
				auto &stats = ProfileStats::Instance();
				stats.PrintSummaryNoLock();
			});
			registered = true;
		}
	}
}

} // namespace vgi
} // namespace duckdb
