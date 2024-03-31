package com.github.clasicrando.common.result

import com.github.clasicrando.common.AutoRelease

/**
 * Container class for the data returned upon completion of a query. Every query must have the
 * number of rows affected, the message sent to the client and the rows returned (empty result if
 * no rows returned).
 *
 * This type is not thread safe and should be accessed by a single thread or coroutine to ensure
 * consistent processing of data.
 */
open class QueryResult(
    val rowsAffected: Long,
    val message: String,
    val rows: ResultSet = ResultSet.EMPTY_RESULT,
) : AutoRelease {
    override fun toString(): String {
        return "QueryResult(rowsAffected=$rowsAffected,message=$message)"
    }

    /** Releases all [rows] found within this result */
    override fun release() {
        rows.release()
    }
}
