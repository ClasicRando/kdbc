package io.github.clasicrando.kdbc.mysql.result

@JvmInline
public value class ColumnFlags(
    private val flags: Int,
) {
    public operator fun get(columnFlags: ColumnFlags): Boolean {
        return (this.flags and columnFlags.flags) == columnFlags.flags
    }

    public operator fun plus(columnFlags: ColumnFlags): ColumnFlags {
        return ColumnFlags(this.flags or columnFlags.flags)
    }

    public companion object {
        internal val NOT_NULL = ColumnFlags(1)
        internal val PRIMARY_KEY = ColumnFlags(2)
        internal val UNIQUE_KEY = ColumnFlags(4)
        internal val MULTIPLE_KEY = ColumnFlags(8)
        internal val BLOB = ColumnFlags(16)
        internal val UNSIGNED = ColumnFlags(32)
        internal val ZEROFILL = ColumnFlags(64)
        internal val BINARY = ColumnFlags(128)
        internal val ENUM = ColumnFlags(256)
        internal val AUTO_INCREMENT = ColumnFlags(512)
        internal val TIMESTAMP = ColumnFlags(1024)
        internal val SET = ColumnFlags(2048)
        internal val NO_DEFAULT_VALUE = ColumnFlags(4096)
        internal val ON_UPDATE_NOW = ColumnFlags(8192)
        internal val NUM = ColumnFlags(32768)
    }
}
