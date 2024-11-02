package io.github.clasicrando.kdbc.mysql.message

@JvmInline
internal value class Capabilities(
    val flags: Long,
) {
    operator fun get(capabilities: Capabilities): Boolean =
        (this.flags and capabilities.flags) == capabilities.flags

    operator fun plus(capabilities: Capabilities): Capabilities =
        Capabilities(this.flags or capabilities.flags)

    operator fun minus(capabilities: Capabilities): Capabilities =
        Capabilities(this.flags and (capabilities.flags.inv()))

    companion object {
        internal val CLIENT_MYSQL = Capabilities(1)
        internal val CLIENT_FOUND_ROWS = Capabilities(2)
        internal val CLIENT_LONG_FLAG = Capabilities(4)
        internal val CLIENT_CONNECT_WITH_DB = Capabilities(8)
        internal val CLIENT_NO_SCHEMA = Capabilities(16)
        internal val CLIENT_COMPRESS = Capabilities(32)
        internal val CLIENT_ODBC = Capabilities(64)
        internal val CLIENT_LOCAL_FILES = Capabilities(128)
        internal val CLIENT_IGNORE_SPACE = Capabilities(256)
        internal val CLIENT_PROTOCOL_41 = Capabilities(512)
        internal val CLIENT_INTERACTIVE = Capabilities(1024)
        internal val CLIENT_SSL = Capabilities(2048)
        internal val CLIENT_IGNORE_SIGPIPE = Capabilities(4096)
        internal val CLIENT_TRANSACTIONS = Capabilities(8192)
        internal val CLIENT_RESERVED = Capabilities(16384)
        internal val CLIENT_SECURE_CONNECTION = Capabilities(1L shl 15)
        internal val CLIENT_MULTI_STATEMENTS = Capabilities(1L shl 16)
        internal val CLIENT_MULTI_RESULTS = Capabilities(1L shl 17)
        internal val CLIENT_PS_MULTI_RESULTS = Capabilities(1L shl 18)
        internal val CLIENT_PLUGIN_AUTH = Capabilities(1L shl 19)
        internal val CLIENT_CONNECT_ATTRS = Capabilities(1L shl 20)
        internal val CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA = Capabilities(1L shl 21)
        internal val CLIENT_CAN_HANDLE_EXPIRED_PASSWORDS = Capabilities(1L shl 22)
        internal val CLIENT_SESSION_TRACK = Capabilities(1L shl 23)
        internal val CLIENT_DEPRECATE_EOF = Capabilities(1L shl 24)
        internal val CLIENT_OPTIONAL_RESULTSET_METADATA = Capabilities(1L shl 25)
        internal val CLIENT_ZSTD_COMPRESSION_ALGORITHM = Capabilities(1L shl 26)
        internal val CLIENT_QUERY_ATTRIBUTES = Capabilities(1L shl 27)
        internal val MULTI_FACTOR_AUTHENTICATION = Capabilities(1L shl 28)
        internal val CLIENT_CAPABILITY_EXTENSION = Capabilities(1L shl 29)
        internal val CLIENT_SSL_VERIFY_SERVER_CERT = Capabilities(1L shl 30)
        internal val CLIENT_REMEMBER_OPTIONS = Capabilities(1L shl 31)
        internal val MARIADB_CLIENT_PROGRESS = Capabilities(1L shl 32)
        internal val MARIADB_CLIENT_MULTI = Capabilities(1L shl 33)
        internal val MARIADB_CLIENT_STMT_BULK_OPERATIONS = Capabilities(1L shl 34)
        internal val MARIADB_CLIENT_EXTENDED_TYPE_INFO = Capabilities(1L shl 35)
        internal val MARIADB_CLIENT_CACHE_METADATA = Capabilities(1L shl 36)
        internal val MARIADB_CLIENT_BULK_UNIT_RESULTS = Capabilities(1L shl 37)
    }
}
