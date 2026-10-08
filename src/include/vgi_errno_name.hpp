// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#pragma once

// errno -> symbolic name for the `errno_name` extra_info field
// (error_key::kErrno). Unknown values come back as the decimal number. Every
// name is #ifdef-guarded so this compiles on any platform's <cerrno>.

#include <cerrno>
#include <string>

namespace duckdb {
namespace vgi {

inline std::string ErrnoName(int err) {
	switch (err) {
#ifdef ECONNREFUSED
	case ECONNREFUSED:
		return "ECONNREFUSED";
#endif
#ifdef ENOENT
	case ENOENT:
		return "ENOENT";
#endif
#ifdef EACCES
	case EACCES:
		return "EACCES";
#endif
#ifdef ETIMEDOUT
	case ETIMEDOUT:
		return "ETIMEDOUT";
#endif
#ifdef ECONNRESET
	case ECONNRESET:
		return "ECONNRESET";
#endif
#ifdef EPIPE
	case EPIPE:
		return "EPIPE";
#endif
#ifdef EADDRINUSE
	case EADDRINUSE:
		return "EADDRINUSE";
#endif
#ifdef ENAMETOOLONG
	case ENAMETOOLONG:
		return "ENAMETOOLONG";
#endif
	default:
		return std::to_string(err);
	}
}

} // namespace vgi
} // namespace duckdb
