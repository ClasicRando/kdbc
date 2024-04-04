package com.github.clasicrando.common.query

import com.github.clasicrando.common.connection.Connection
import com.github.clasicrando.common.result.StatementResult

/**
 * Default implementation of [QueryBatch] that naively executes each query sequentially and
 * aggregates the results into a single [StatementResult]. This is only to be leveraged if the
 * database does not provide a more efficient way to batch queries.
 */
class DefaultQueryBatch(connection: Connection) : QueryBatch(connection) {
    override suspend fun executeQueriesAggregating(
        queries: List<Pair<String, List<Any?>>>,
    ): StatementResult {
        checkNotNull(connection) { "QueryBatch already released its Connection" }
        val statementResultBuilder = StatementResult.Builder()
        try {
            for ((query, parameters) in queries) {
                val statementResult = connection!!.sendPreparedStatement(query, parameters)
                for (result in statementResult) {
                    statementResultBuilder.addQueryResult(result)
                }
            }
        } catch (ex: Throwable) {
            statementResultBuilder.build().release()
            throw ex
        }
        return statementResultBuilder.build()
    }
}
