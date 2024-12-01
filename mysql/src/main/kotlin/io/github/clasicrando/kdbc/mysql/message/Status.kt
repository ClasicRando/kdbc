package io.github.clasicrando.kdbc.mysql.message

/**
 * Server status bitflag type. Internally it's an [Int] since the actual value is a [Short] so [Int]
 * can hold all the values needed and allow for bitwise operations.
 */
@JvmInline
internal value class Status(val flags: Int) {
    constructor(flags: Short) : this(flags.toInt() and 0xff_ff)

    /**
     * Returns true if the specific status is present in this value (i.e. [Int.and] equals the
     * supplied [status])
     */
    operator fun get(status: Status): Boolean {
        return (this.flags and status.flags) == status.flags
    }

    /**
     * Add all bits from the donor [Status] while preserving all exists flags. This is equivalent to
     * a [Int.or]
     */
    operator fun plus(status: Status): Status {
        return Status(this.flags or status.flags)
    }

    /** Remove all [status] bits supplied by settings the bit positions to zero */
    operator fun minus(status: Status): Status {
        return Status(this.flags and (status.flags.inv()))
    }

    /** `and` these two [Status] to get a result that is [Int.and] */
    infix fun and(status: Status): Status {
        return Status(this.flags and status.flags)
    }

    @Suppress("unused")
    companion object {
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
    }
}
