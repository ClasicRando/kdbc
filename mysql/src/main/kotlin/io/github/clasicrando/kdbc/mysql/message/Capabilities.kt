package io.github.clasicrando.kdbc.mysql.message

import kotlinx.io.Sink
import kotlinx.io.writeUIntLe

/**
 * Server/Client capabilities bitflag type. Internally it's just a [ULong]. Utility methods are
 * provided to compare against other known flags.
 */
@JvmInline
internal value class Capabilities(val flags: ULong) {
    /**
     * Returns true if the specific capability is present in this value (i.e. [ULong.and] equals the
     * supplied [capabilities])
     */
    operator fun get(capabilities: Capabilities): Boolean {
        return (this.flags and capabilities.flags) == capabilities.flags
    }

    /**
     * Add all bits from the donor [Capabilities] while preserving all exists flags. This is
     * equivalent to a [ULong.or]
     */
    operator fun plus(capabilities: Capabilities): Capabilities {
        return Capabilities(this.flags or capabilities.flags)
    }

    /** Remove all [capabilities] supplied by settings the bit positions to zero */
    operator fun minus(capabilities: Capabilities): Capabilities {
        return Capabilities(this.flags and capabilities.flags.inv().and(0xff_ff_ff_ff_ff_ff_ff_ffu))
    }

    /** `and` these two [Capabilities] to get a result that is [ULong.and] */
    infix fun and(capabilities: Capabilities): Capabilities {
        return Capabilities(this.flags and capabilities.flags)
    }

    /** Write this value to the [sink] as the first 32 bit flags in Little Endian order */
    fun writeAsIntLe(sink: Sink) {
        sink.writeUIntLe(flags.toUInt())
    }

    @Suppress("unused")
    companion object {
        internal val CLIENT_MYSQL = Capabilities(1u)
        internal val CLIENT_FOUND_ROWS = Capabilities(2u)
        internal val CLIENT_LONG_FLAG = Capabilities(4u)
        internal val CLIENT_CONNECT_WITH_DB = Capabilities(8u)
        internal val CLIENT_NO_SCHEMA = Capabilities(16u)
        internal val CLIENT_COMPRESS = Capabilities(32u)
        internal val CLIENT_ODBC = Capabilities(64u)
        internal val CLIENT_LOCAL_FILES = Capabilities(128u)
        internal val CLIENT_IGNORE_SPACE = Capabilities(256u)
        internal val CLIENT_PROTOCOL_41 = Capabilities(512u)
        internal val CLIENT_INTERACTIVE = Capabilities(1024u)
        internal val CLIENT_SSL = Capabilities(2048u)
        internal val CLIENT_IGNORE_SIGPIPE = Capabilities(4096u)
        internal val CLIENT_TRANSACTIONS = Capabilities(8192u)
        internal val CLIENT_RESERVED = Capabilities(16384u)
        internal val CLIENT_SECURE_CONNECTION = Capabilities(1uL shl 15) // 32768
        internal val CLIENT_MULTI_STATEMENTS = Capabilities(1uL shl 16) // 65536
        internal val CLIENT_MULTI_RESULTS = Capabilities(1uL shl 17) // 131072
        internal val CLIENT_PS_MULTI_RESULTS = Capabilities(1uL shl 18) // 262144
        internal val CLIENT_PLUGIN_AUTH = Capabilities(1uL shl 19) // 524288
        internal val CLIENT_CONNECT_ATTRS = Capabilities(1uL shl 20) // 1048576
        internal val CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA = Capabilities(1uL shl 21) // 2097152
        internal val CLIENT_CAN_HANDLE_EXPIRED_PASSWORDS = Capabilities(1uL shl 22) // 4194304
        internal val CLIENT_SESSION_TRACK = Capabilities(1uL shl 23) // 8388608
        internal val CLIENT_DEPRECATE_EOF = Capabilities(1uL shl 24) // 16777216
        internal val CLIENT_OPTIONAL_RESULTSET_METADATA = Capabilities(1uL shl 25) // 33554432
        internal val CLIENT_ZSTD_COMPRESSION_ALGORITHM = Capabilities(1uL shl 26) // 67108864
        internal val CLIENT_QUERY_ATTRIBUTES = Capabilities(1uL shl 27) // 134217728
        internal val MULTI_FACTOR_AUTHENTICATION = Capabilities(1uL shl 28) // 268435456
        internal val CLIENT_CAPABILITY_EXTENSION = Capabilities(1uL shl 29) // 536870912
        internal val CLIENT_SSL_VERIFY_SERVER_CERT = Capabilities(1uL shl 30) // 1073741824
        internal val CLIENT_REMEMBER_OPTIONS = Capabilities(1uL shl 31) // 2147483648
        internal val MARIADB_CLIENT_PROGRESS = Capabilities(1uL shl 32) // 4294967296
        internal val MARIADB_CLIENT_MULTI = Capabilities(1uL shl 33) // 8589934592
        internal val MARIADB_CLIENT_STMT_BULK_OPERATIONS = Capabilities(1uL shl 34) // 17179869184
        internal val MARIADB_CLIENT_EXTENDED_TYPE_INFO = Capabilities(1uL shl 35) // 34359738368
        internal val MARIADB_CLIENT_CACHE_METADATA = Capabilities(1uL shl 36) // 68719476736
        internal val MARIADB_CLIENT_BULK_UNIT_RESULTS = Capabilities(1uL shl 37) // 137438953472
    }
}
