package io.github.clasicrando.kdbc.mysql.message

import io.github.clasicrando.kdbc.core.connection.IntBitFlags

/** Server status bitflag type. Standard mask values */
@Suppress("unused")
internal object Status {
    internal val SERVER_STATUS_IN_TRANS = IntBitFlags(1)
    internal val SERVER_STATUS_AUTOCOMMIT = IntBitFlags(2)
    internal val SERVER_MORE_RESULTS_EXISTS = IntBitFlags(8)
    internal val SERVER_QUERY_NO_GOOD_INDEX_USED = IntBitFlags(16)
    internal val SERVER_QUERY_NO_INDEX_USED = IntBitFlags(32)
    internal val SERVER_STATUS_CURSOR_EXISTS = IntBitFlags(64)
    internal val SERVER_STATUS_LAST_ROW_SENT = IntBitFlags(128)
    internal val SERVER_STATUS_DB_DROPPED = IntBitFlags(256)
    internal val SERVER_STATUS_NO_BACKSLASH_ESCAPES = IntBitFlags(512)
    internal val SERVER_STATUS_METADATA_CHANGED = IntBitFlags(1024)
    internal val SERVER_QUERY_WAS_SLOW = IntBitFlags(2048)
    internal val SERVER_PS_OUT_PARAMS = IntBitFlags(4096)
    internal val SERVER_STATUS_IN_TRANS_READONLY = IntBitFlags(8192)
    internal val SERVER_SESSION_STATE_CHANGED = IntBitFlags(16384)
}
