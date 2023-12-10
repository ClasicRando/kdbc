package com.github.clasicrando.common.connection

import com.github.clasicrando.common.result.QueryResult

/**
 * Query [Executor] for all database connection like objects. This provides the basis for any
 * object that can make query calls to a database including (but not limited to):
 * - [Connection]
 * - [ConnectionPool][com.github.clasicrando.common.pool.ConnectionPool]
 */
interface Executor {
    /** Send a raw query with no parameters, returning a [QueryResult] */
    suspend fun sendQuery(query: String): QueryResult
    /**
     * Send a prepared statement with [parameters], returning a [QueryResult]. If you do not think
     * you will need to use the statement again you can specify true for [release] to force the
     * database to release the prepared statement on the server side.
     */
    suspend fun sendPreparedStatement(
        query: String,
        parameters: List<Any?>,
        release: Boolean = false,
    ): QueryResult
}

/** Run [Executor.sendQuery] wrapped in a [runCatching] block */
suspend fun Executor.sendQueryCatching(query: String): Result<QueryResult> {
    return runCatching {
        sendQuery(query)
    }
}

/** Run [Executor.sendPreparedStatement] wrapped in a [runCatching] block */
suspend fun Executor.sendPreparedStatementCatching(
    query: String,
    parameters: List<Any?>,
    release: Boolean = false,
): Result<QueryResult> {
    return runCatching {
        sendPreparedStatement(query, parameters, release)
    }
}
