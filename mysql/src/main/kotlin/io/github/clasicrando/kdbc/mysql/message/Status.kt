package io.github.clasicrando.kdbc.mysql.message

@JvmInline
internal value class Status(
    val flags: Short,
)

internal val SERVER_STATUS_IN_TRANS = Status(1)
internal val SERVER_STATUS_AUTOCOMMIT = Status(2)
internal val SERVER_MORE_RESULTS_EXISTS = Status(8)
internal val SERVER_QUERY_NO_GOOD_INDEX_USED = Status(16)
internal val SERVER_QUERY_NO_INDEX_USED = Status(32)
internal val SERVER_STATUS_CURSOR_EXISTS = Status(64)
internal val SERVER_STATUS_LAST_ROW_SENT = Status(128)
internal val SERVER_STATUS_DB_DROPPED = Status(256)
internal val SERVER_STATUS_NO_BACKSLASH_ESCAPES = Status(512)
internal val SERVER_STATUS_METADATA_CHANGED = Status(1024)
internal val SERVER_QUERY_WAS_SLOW = Status(2048)
internal val SERVER_PS_OUT_PARAMS = Status(4096)
internal val SERVER_STATUS_IN_TRANS_READONLY = Status(8192)
internal val SERVER_SESSION_STATE_CHANGED = Status(16384)
