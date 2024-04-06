package com.github.clasicrando.postgresql.query

import com.github.clasicrando.common.query.BlockingQueryBatch
import com.github.clasicrando.common.result.StatementResult
import com.github.clasicrando.postgresql.connection.PgBlockingConnection

/**
 * Postgresql implementation of a [BlockingQueryBatch]. Uses query pipelining to execute all
 * prepared statements as a pipeline to optimize round trips to the server.
 *
 * @see PgBlockingConnection.pipelineQueries
 */
class PgBlockingQueryBatch(connection: PgBlockingConnection) : BlockingQueryBatch(connection) {
    /** Sync all parameter provided to the [PgBlockingConnection.pipelineQueries] method */
    var syncAll: Boolean = true

    override fun executeQueriesAggregating(
        queries: List<Pair<String, List<Any?>>>,
    ): StatementResult {
        checkNotNull(connection) { "QueryBatch already released its Connection" }
        check(connection is PgBlockingConnection) {
            "PgQueryBatch's connection is not PgConnection. This should never happen"
        }
        val statementResultBuilder = StatementResult.Builder()
        try {
            val pipelineQueries = Array(queries.size) { queries[it] }
            val queryResults = (connection as PgBlockingConnection)
                .pipelineQueries(syncAll = syncAll, queries = pipelineQueries)
            for (queryResult in queryResults) {
                statementResultBuilder.addQueryResult(queryResult)
            }
        } catch (ex: Throwable) {
            statementResultBuilder.build().release()
            throw ex
        }
        return statementResultBuilder.build()
    }
}
