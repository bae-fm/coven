pub(crate) fn install_platform_connection_opener() {
    coven_core::database::register_platform_connection_opener(
        coven_core::database::open_native_connection,
    );
}
