# Keep the standard Emscripten toolchain/architecture settings, but give pthread
# dependencies a separate package ABI and cache from the single-threaded builds.
include("${VCPKG_ROOT_DIR}/triplets/community/wasm32-emscripten.cmake")
list(APPEND VCPKG_HASH_ADDITIONAL_FILES
    "${VCPKG_ROOT_DIR}/triplets/community/wasm32-emscripten.cmake")

string(APPEND VCPKG_C_FLAGS " -pthread")
string(APPEND VCPKG_CXX_FLAGS " -pthread")
string(APPEND VCPKG_LINKER_FLAGS " -pthread")
