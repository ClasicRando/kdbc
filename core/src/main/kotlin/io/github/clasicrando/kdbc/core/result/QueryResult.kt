package io.github.clasicrando.kdbc.core.result

/**
 * Container class for the data returned upon completion of a query. Every query must have the
 * number of rows affected, the message sent to the client and the rows returned (empty result if no
 * rows returned).
 *
 * This type is not thread safe and should be accessed by a single thread or coroutine to ensure
 * consistent processing of data.
 */
public open class QueryResult(public val rowsAffected: Long, public val message: String) {
    public fun merge(other: QueryResult): QueryResult {
        return QueryResult(
            rowsAffected = this.rowsAffected + other.rowsAffected,
            message = "${this.message},${other.message}".trim(','),
        )
    }

    override fun toString(): String = "QueryResult(rowsAffected=$rowsAffected,message=$message)"
}
