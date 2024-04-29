package io.github.clasicrando.kdbc.postgresql.query

import io.github.clasicrando.kdbc.core.query.BasePreparedQueryBatch
import io.github.clasicrando.kdbc.core.query.PreparedQueryBatch
import io.github.clasicrando.kdbc.core.result.StatementResult
import io.github.clasicrando.kdbc.postgresql.connection.PgConnection

/**
 * Postgresql implementation of a [PreparedQueryBatch]. Uses query pipelining to execute all
 * prepared statements as a pipeline to optimize round trips to the server. This allows for sending
 * multiple prepared queries at once to the server, so you do not need to wait for previous queries
 * to complete to request another result.
 *
 * ```
 * Regular Pipelined
 * | Client         | Server          |    | Client         | Server          |
 * |----------------|-----------------|    |----------------|-----------------|
 * | send query 1   |                 |    | send query 1   |                 |
 * |                | process query 1 |    | send query 2   | process query 1 |
 * | receive rows 1 |                 |    | send query 3   | process query 2 |
 * | send query 2   |                 |    | receive rows 1 | process query 3 |
 * |                | process query 2 |    | receive rows 2 |                 |
 * | receive rows 2 |                 |    | receive rows 3 |                 |
 * | send query 3   |                 |
 * |                | process query 3 |
 * | receive rows 3 |                 |
 * ```
 *
 * This can reduce server round trips, however there is one limitation to this client's
 * implementation of query pipelining. Currently, the client takes an all or nothing approach
 * where sync messages are sent after each query (instructing an autocommit by the server
 * unless already in an open transaction) by default. To override this behaviour, allowing all
 * statements after the failed one to be skipped and all previous statement changes to be
 * rolled back, change the [syncAll] parameter to false.
 *
 * If you are sure each one of your statements do not impact each other and can be handled in
 * separate transactions, keep the [syncAll] as default and catch exception thrown during
 * query execution. Alternatively, you can also manually begin a transaction using
 * [io.github.clasicrando.kdbc.core.connection.Connection.begin] and handle the transaction state
 * of your connection yourself. In that case, any sync message sent to the server does not cause
 * implicit transactional behaviour.
 *
 * @see PgConnection.pipelineQueries
 */
class PgPreparedQueryBatch(connection: PgConnection) : BasePreparedQueryBatch<PgConnection>(connection) {
    /** Sync all parameter provided to the [PgConnection.pipelineQueries] method */
    var syncAll: Boolean = true

    override suspend fun vendorExecuteQueriesAggregating(): StatementResult {
        checkNotNull(connection) { "QueryBatch already released its Connection" }
        val statementResultBuilder = StatementResult.Builder()
        try {
            val pipelineQueries = Array(queries.size) {
                queries[it].sql to queries[it].parameters
            }
            connection!!
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
