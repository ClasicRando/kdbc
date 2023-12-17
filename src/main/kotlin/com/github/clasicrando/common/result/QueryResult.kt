package com.github.clasicrando.common.result

/**
 * Container class for the data returned upon completion of a query. Every query must have the
 * number of rows affected, the message sent to the client and the rows returned (empty result if
 * no rows returned/affected).
 */
open class QueryResult(
    val rowsAffected: Long,
    val message: String,
    val rows: ResultSet = ResultSet.EMPTY_RESULT,
) {
    override fun toString(): String {
        return "QueryResult(rowsAffected=$rowsAffected,message=$message)"
    }
}
