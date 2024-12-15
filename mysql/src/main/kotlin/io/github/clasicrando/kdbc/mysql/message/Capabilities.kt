package io.github.clasicrando.kdbc.mysql.message

import io.github.clasicrando.kdbc.core.connection.LongBitFlags

/** Server/Client capabilities bitflags. Provides the know values as [LongBitFlags] */
@Suppress("unused")
internal object Capabilities {
    internal val CLIENT_MYSQL = LongBitFlags(1)
    internal val CLIENT_FOUND_ROWS = LongBitFlags(2)
    internal val CLIENT_LONG_FLAG = LongBitFlags(4)
    internal val CLIENT_CONNECT_WITH_DB = LongBitFlags(8)
    internal val CLIENT_NO_SCHEMA = LongBitFlags(16)
    internal val CLIENT_COMPRESS = LongBitFlags(32)
    internal val CLIENT_ODBC = LongBitFlags(64)
    internal val CLIENT_LOCAL_FILES = LongBitFlags(128)
    internal val CLIENT_IGNORE_SPACE = LongBitFlags(256)
    internal val CLIENT_PROTOCOL_41 = LongBitFlags(512)
    internal val CLIENT_INTERACTIVE = LongBitFlags(1024)
    internal val CLIENT_SSL = LongBitFlags(2048)
    internal val CLIENT_IGNORE_SIGPIPE = LongBitFlags(4096)
    internal val CLIENT_TRANSACTIONS = LongBitFlags(8192)
    internal val CLIENT_RESERVED = LongBitFlags(16384)
    internal val CLIENT_SECURE_CONNECTION = LongBitFlags(1L shl 15) // 32768
    internal val CLIENT_MULTI_STATEMENTS = LongBitFlags(1L shl 16) // 65536
    internal val CLIENT_MULTI_RESULTS = LongBitFlags(1L shl 17) // 131072
    internal val CLIENT_PS_MULTI_RESULTS = LongBitFlags(1L shl 18) // 262144
    internal val CLIENT_PLUGIN_AUTH = LongBitFlags(1L shl 19) // 524288
    internal val CLIENT_CONNECT_ATTRS = LongBitFlags(1L shl 20) // 1048576
    internal val CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA = LongBitFlags(1L shl 21) // 2097152
    internal val CLIENT_CAN_HANDLE_EXPIRED_PASSWORDS = LongBitFlags(1L shl 22) // 4194304
    internal val CLIENT_SESSION_TRACK = LongBitFlags(1L shl 23) // 8388608
    internal val CLIENT_DEPRECATE_EOF = LongBitFlags(1L shl 24) // 16777216
    internal val CLIENT_OPTIONAL_RESULTSET_METADATA = LongBitFlags(1L shl 25) // 33554432
    internal val CLIENT_ZSTD_COMPRESSION_ALGORITHM = LongBitFlags(1L shl 26) // 67108864
    internal val CLIENT_QUERY_ATTRIBUTES = LongBitFlags(1L shl 27) // 134217728
    internal val MULTI_FACTOR_AUTHENTICATION = LongBitFlags(1L shl 28) // 268435456
    internal val CLIENT_CAPABILITY_EXTENSION = LongBitFlags(1L shl 29) // 536870912
    internal val CLIENT_SSL_VERIFY_SERVER_CERT = LongBitFlags(1L shl 30) // 1073741824
    internal val CLIENT_REMEMBER_OPTIONS = LongBitFlags(1L shl 31) // 2147483648
    internal val MARIADB_CLIENT_PROGRESS = LongBitFlags(1L shl 32) // 4294967296
    internal val MARIADB_CLIENT_MULTI = LongBitFlags(1L shl 33) // 8589934592
    internal val MARIADB_CLIENT_STMT_BULK_OPERATIONS = LongBitFlags(1L shl 34) // 17179869184
    internal val MARIADB_CLIENT_EXTENDED_TYPE_INFO = LongBitFlags(1L shl 35) // 34359738368
    internal val MARIADB_CLIENT_CACHE_METADATA = LongBitFlags(1L shl 36) // 68719476736
    internal val MARIADB_CLIENT_BULK_UNIT_RESULTS = LongBitFlags(1L shl 37) // 137438953472
}
