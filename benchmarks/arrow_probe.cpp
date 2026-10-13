// Opt-in diagnostic hook in the SAME module as VGI's statically linked Arrow.
// Linking Arrow into the host would inspect a different default memory pool.
// Not compiled into normal extension builds and not exposed as a SQL function.
#include <arrow/memory_pool.h>
#include <arrow/api.h>
#include "vgi_arrow_utils.hpp"
#include <cstring>
#include <sstream>
#include <string>

extern "C" __attribute__((visibility("default"))) const char *vgi_benchmark_arrow_pool(int release_unused) {
    static thread_local std::string result;
    try {
        auto pool = arrow::default_memory_pool();
        if (release_unused == 1) pool->ReleaseUnused();
        bool schema_reuse_passed = true;
        if (release_unused == 2) {
            auto before = pool->bytes_allocated();
            {
                duckdb::ArrowSchemaWrapper wrapper;
                for (int i = 0; i < 128; i++) {
                    auto schema = arrow::schema({arrow::field("value", i % 2 ? arrow::utf8() : arrow::list(arrow::utf8()))});
                    duckdb::vgi::ExportSchema(schema, wrapper);
                    schema_reuse_passed = schema_reuse_passed && wrapper.arrow_schema.release &&
                        wrapper.arrow_schema.n_children == 1 &&
                        std::strcmp(wrapper.arrow_schema.children[0]->format, i % 2 ? "u" : "+l") == 0;
                }
            }
            schema_reuse_passed = schema_reuse_passed && pool->bytes_allocated() == before;
        }
        std::ostringstream out;
        out << "{\"backend\":\"" << pool->backend_name() << "\",\"bytes_allocated\":" << pool->bytes_allocated()
            << ",\"max_memory\":" << pool->max_memory()
            << ",\"total_bytes_allocated\":" << pool->total_bytes_allocated()
            << ",\"num_allocations\":" << pool->num_allocations();
        if (release_unused == 2) out << ",\"schema_reuse_passed\":" << (schema_reuse_passed ? "true" : "false");
        out << "}";
        result = out.str();
    } catch (...) {
        result = "{\"error\":\"Arrow allocator diagnostic failed\"}";
    }
    return result.c_str();
}
