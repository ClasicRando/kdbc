package io.github.clasicrando.kdbc.core.query

import io.github.clasicrando.kdbc.core.connection.SuspendingConnection
import io.github.clasicrando.kdbc.core.result.StatementResult

/**
 * Base implementation of a [SuspendingPreparedQueryBatch]. Delegates executing the queries to the
 * database specific implementation.
 */
abstract class BaseSuspendingPreparedQueryBatch<C : SuspendingConnection>(
    protected var connection: C?,
) : SuspendingPreparedQueryBatch {
    protected val queries: MutableList<SuspendingPreparedQuery> = mutableListOf()

    /**
     * Implementation specific method to execute and aggregate the results returned from the
     * database server into a single [StatementResult].
     */
    protected abstract suspend fun vendorExecuteQueriesAggregating(): StatementResult

    /**
     * Execute all provided queries into a single [StatementResult] and allow for processing each
     * [io.github.clasicrando.kdbc.core.result.QueryResult] returned.
     */
    final override suspend fun executeQueries(): StatementResult {
        checkNotNull(connection) { "QueryBatch already released its Connection" }
        if (queries.isEmpty()) {
            return StatementResult(emptyList())
        }
        return vendorExecuteQueriesAggregating()
    }

    /**
     * Create a new [SuspendingPreparedQuery] using the provided [query] [String] and add it to the
     * queries contained within this batch of queries. Returns the newly created
     * [SuspendingPreparedQuery] so parameters can be added if needed.
     *
     * @throws IllegalStateException if the batch has already been released
     */
    final override fun addPreparedQuery(query: String): SuspendingPreparedQuery {
        checkNotNull(connection) { "QueryBatch already released its Connection" }
        val result = connection!!.createPreparedQuery(query)
        queries += result
        return result
    }

    override fun release() {
        connection = null
    }
}
