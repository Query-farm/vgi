// © Copyright 2025, 2026 Query Farm LLC - https://query.farm

#include "vgi_client_timing.hpp"

#include <cstdio>
#include <cstdlib>

namespace duckdb {
namespace vgi {

ClientTiming &ClientTiming::Instance() {
	static ClientTiming instance;
	return instance;
}

bool ClientTiming::Enabled() {
	static const bool enabled = std::getenv("VGI_RPC_CLIENT_TIMING") != nullptr;
	return enabled;
}

namespace {

// Prints the accumulated breakdown at process exit (the duckdb CLI runs its
// queries then tears down). `read` includes the worker's busy time + transport,
// so subtract the worker's `[vgi-shm] timeline` busy figure to isolate handoff.
struct TimingDumper {
	~TimingDumper() {
		if (!ClientTiming::Enabled()) {
			return;
		}
		auto &t = ClientTiming::Instance();
		const double ci = static_cast<double>(t.convert_in_ns.load());
		const double w = static_cast<double>(t.write_ns.load());
		const double r = static_cast<double>(t.read_ns.load());
		const double sc = static_cast<double>(t.schema_ns.load());
		const double co = static_cast<double>(t.convert_out_ns.load());
		const double total = ci + w + r + sc + co;
		if (total <= 0.0) {
			return;
		}
		fprintf(stderr,
		        "[vgi-client] timeline: batches=%llu total=%.1f ms | "
		        "convert_in=%.1f ms (%.0f%%) | write=%.1f ms (%.0f%%) | "
		        "read(worker+transport)=%.1f ms (%.0f%%) | schema=%.1f ms (%.0f%%) | "
		        "convert_out=%.1f ms (%.0f%%)\n",
		        static_cast<unsigned long long>(t.batches.load()), total / 1e6, ci / 1e6, 100.0 * ci / total,
		        w / 1e6, 100.0 * w / total, r / 1e6, 100.0 * r / total, sc / 1e6, 100.0 * sc / total, co / 1e6,
		        100.0 * co / total);
	}
};

TimingDumper g_timing_dumper;

} // namespace

} // namespace vgi
} // namespace duckdb
