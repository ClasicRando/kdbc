package com.github.clasicrando.common.connection

import com.github.clasicrando.common.result.QueryResult
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.toList

/**
 * Query [Executor] for all database connection like objects. This provides the basis for any
 * object that can make query calls to a database including (but not limited to):
 * - [Connection]
 * - [ConnectionPool][com.github.clasicrando.common.pool.ConnectionPool]
 */
interface Executor {
    /** Send a raw query with no parameters, returning a [QueryResult] */
    suspend fun sendQueryFlow(query: String): Flow<QueryResult>
    suspend fun sendQuery(query: String): Iterable<QueryResult> = sendQueryFlow(query).toList()
    /** Send a prepared statement with [parameters], returning a [QueryResult]. */
    suspend fun sendPreparedStatementFlow(
        query: String,
        parameters: List<Any?>,
    ): Flow<QueryResult>
    suspend fun sendPreparedStatement(
        query: String,
        parameters: List<Any?>,
    ): Iterable<QueryResult> = sendPreparedStatementFlow(query, parameters).toList()
}
