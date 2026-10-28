# Matches nanoarrow's original runtime revision. No verifier/source patches.
vcpkg_check_linkage(ONLY_STATIC_LIBRARY)
vcpkg_from_github(
    OUT_SOURCE_PATH SOURCE_PATH
    REPO dvidelabs/flatcc
    REF e3e44836c5f625b5532586ddce895f8b5e36a212
    SHA512 f82687188cdf1f9edd0a3ef535009afad9a3019a7d2a98c04955f24a7b430a66f1bd24bd75c595abd1ca5c9b2558dcc1af7565c8872dfdc01d9ad4cfb4d61aa6
    PATCHES install-targets.patch
)
vcpkg_cmake_configure(
    SOURCE_PATH "${SOURCE_PATH}"
    OPTIONS
        -DFLATCC_INSTALL=ON
        -DFLATCC_ALLOW_WERROR=OFF
        -DFLATCC_TEST=OFF
        -DFLATCC_CXX_TEST=OFF
        -DFLATCC_DEBUG_CLANG_SANITIZE=OFF
        -DFLATCC_RTONLY=ON
        -DCMAKE_POSITION_INDEPENDENT_CODE=ON
)
vcpkg_cmake_install()
vcpkg_copy_pdbs()
vcpkg_cmake_config_fixup(PACKAGE_NAME flatcc CONFIG_PATH share/flatcc)
file(REMOVE_RECURSE "${CURRENT_PACKAGES_DIR}/debug/include")
vcpkg_install_copyright(FILE_LIST "${SOURCE_PATH}/LICENSE")
