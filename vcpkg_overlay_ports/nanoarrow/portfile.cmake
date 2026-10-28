# Same IPC implementation as the former bundle, plus CMake package support.
vcpkg_check_linkage(ONLY_STATIC_LIBRARY)
vcpkg_from_github(
    OUT_SOURCE_PATH SOURCE_PATH
    REPO Query-farm/arrow-nanoarrow
    REF f2aca3b4182e115745f1971e8d85a66594f28679
    SHA512 8bca258c46a401170b30dda349bb8ae9ab8f1c76e1ee049f50f731b90f1df4affdd423cb1fb602b73dd65f34f208a7e4350303af0b24e1e76c4684cbd86e057a
)
# Enforce use of the separately packaged, unmodified FlatCC runtime.
file(REMOVE_RECURSE "${SOURCE_PATH}/thirdparty/flatcc")
set(VGI_IPC_COMPRESSION ON)
if(VCPKG_TARGET_IS_EMSCRIPTEN)
    set(VGI_IPC_COMPRESSION OFF)
endif()
vcpkg_cmake_configure(
    SOURCE_PATH "${SOURCE_PATH}"
    OPTIONS
        -DNANOARROW_IPC=ON
        -DNANOARROW_NAMESPACE=VgiNanoarrow
        -DNANOARROW_USE_PACKAGE_DEPENDENCIES=ON
        -DNANOARROW_IPC_WITH_ZSTD=${VGI_IPC_COMPRESSION}
        -DNANOARROW_IPC_WITH_LZ4=${VGI_IPC_COMPRESSION}
        -DNANOARROW_INSTALL_SHARED=OFF
        -DNANOARROW_DEBUG_EXTRA_WARNINGS=OFF
        -DCMAKE_C_STANDARD=11
        -DCMAKE_POSITION_INDEPENDENT_CODE=ON
)
vcpkg_cmake_install()
vcpkg_copy_pdbs()
vcpkg_cmake_config_fixup(PACKAGE_NAME nanoarrow CONFIG_PATH lib/cmake/nanoarrow)
file(REMOVE_RECURSE "${CURRENT_PACKAGES_DIR}/debug/include")
vcpkg_install_copyright(FILE_LIST "${SOURCE_PATH}/LICENSE.txt")
