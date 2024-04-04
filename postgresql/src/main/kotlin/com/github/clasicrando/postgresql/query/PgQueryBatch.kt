package com.github.clasicrando.postgresql.query

import com.github.clasicrando.common.query.QueryBatch
import com.github.clasicrando.common.result.StatementResult
import com.github.clasicrando.postgresql.connection.PgConnection

/**
 * Postgresql implementation of a [QueryBatch]. Uses query pipelining to execute all prepared
 * statements as a pipeline to optimize round trips to the server.
 *
 * @see PgConnection.pipelineQueries
 */
class PgQueryBatch(connection: PgConnection) : QueryBatch(connection) {
    /** Sync all parameter provided to the [PgConnection.pipelineQueries] method */
    var syncAll: Boolean = true

    override suspend fun executeQueriesAggregating(
        queries: List<Pair<String, List<Any?>>>,
    ): StatementResult {
        checkNotNull(connection) { "QueryBatch already released its Connection" }
        check(connection is PgConnection) {
            "PgQueryBatch's connection is not PgConnection. This should never happen"
        }
        val statementResultBuilder = StatementResult.Builder()
        try {
            val pipelineQueries = Array(queries.size) { queries[it] }
            (connection as PgConnection)
                .pipelineQueries(syncAll = syncAll, queries = pipelineQueries)
                .collect {
                    statementResultBuilder.addQueryResult(it)
                }
        } catch (ex: Throwable) {
            statementResultBuilder.build().release()
            throw ex
        }
        return statementResultBuilder.build()
    }
}
