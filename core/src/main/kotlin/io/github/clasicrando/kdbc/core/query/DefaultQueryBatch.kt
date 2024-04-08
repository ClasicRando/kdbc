package io.github.clasicrando.kdbc.core.query

import io.github.clasicrando.kdbc.core.connection.Connection
import io.github.clasicrando.kdbc.core.result.StatementResult

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
