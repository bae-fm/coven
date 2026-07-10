pub(crate) fn install_platform_connection_opener() {
    coven_core::database::register_platform_connection_opener(
        coven_core::database::open_native_connection,
    );
    coven_core::database::register_platform_readonly_connection_opener(
        coven_core::database::open_native_connection_read_only,
    );
}
