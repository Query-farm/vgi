# Chainloaded by wasm32-emscripten-threads.cmake. vcpkg seeds CMAKE_<LANG>_FLAGS
# from VCPKG_<LANG>_FLAGS before including the chainload toolchain, and
# Emscripten.cmake then overwrites them, so -pthread must be appended after the
# include. Without it the dependencies lack the wasm atomics/bulk-memory features
# and wasm-ld rejects the extension's --shared-memory link.
include("$ENV{EMSDK}/upstream/emscripten/cmake/Modules/Platform/Emscripten.cmake")

foreach(_lang C CXX)
	string(APPEND CMAKE_${_lang}_FLAGS " -pthread")
	string(APPEND CMAKE_${_lang}_FLAGS_INIT " -pthread")
endforeach()
foreach(_link EXE SHARED MODULE)
	string(APPEND CMAKE_${_link}_LINKER_FLAGS " -pthread")
	string(APPEND CMAKE_${_link}_LINKER_FLAGS_INIT " -pthread")
endforeach()
