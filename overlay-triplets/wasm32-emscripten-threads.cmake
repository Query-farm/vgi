# Keep the standard Emscripten toolchain/architecture settings, but give pthread
# dependencies a separate package ABI and cache from the single-threaded builds.
include("${VCPKG_ROOT_DIR}/triplets/community/wasm32-emscripten.cmake")
list(APPEND VCPKG_HASH_ADDITIONAL_FILES
    "${VCPKG_ROOT_DIR}/triplets/community/wasm32-emscripten.cmake"
    "${CMAKE_CURRENT_LIST_DIR}/wasm32-emscripten-threads-toolchain.cmake")

# VCPKG_C_FLAGS alone does not reach the compiler: Emscripten.cmake overwrites
# CMAKE_C_FLAGS. The chainloaded wrapper appends -pthread after it.
set(VCPKG_CHAINLOAD_TOOLCHAIN_FILE "${CMAKE_CURRENT_LIST_DIR}/wasm32-emscripten-threads-toolchain.cmake")
set(VCPKG_ENV_PASSTHROUGH_UNTRACKED EMSDK PATH)
