package io.github.clasicrando.kdbc.mysql.result

import io.github.clasicrando.kdbc.core.connection.IntBitFlags

/** Column attribute bit flags. Standard mask values */
@Suppress("unused")
public object ColumnFlags {
    internal val NOT_NULL = IntBitFlags(1)
    internal val PRIMARY_KEY = IntBitFlags(2)
    internal val UNIQUE_KEY = IntBitFlags(4)
    internal val MULTIPLE_KEY = IntBitFlags(8)
    internal val BLOB = IntBitFlags(16)
    internal val UNSIGNED = IntBitFlags(32)
    internal val ZEROFILL = IntBitFlags(64)
    internal val BINARY = IntBitFlags(128)
    internal val ENUM = IntBitFlags(256)
    internal val AUTO_INCREMENT = IntBitFlags(512)
    internal val TIMESTAMP = IntBitFlags(1024)
    internal val SET = IntBitFlags(2048)
    internal val NO_DEFAULT_VALUE = IntBitFlags(4096)
    internal val ON_UPDATE_NOW = IntBitFlags(8192)
    internal val NUM = IntBitFlags(32768)
}
