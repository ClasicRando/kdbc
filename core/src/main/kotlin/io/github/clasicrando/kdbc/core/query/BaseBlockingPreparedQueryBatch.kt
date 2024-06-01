package io.github.clasicrando.kdbc.core.query

import io.github.clasicrando.kdbc.core.connection.BlockingConnection
import io.github.clasicrando.kdbc.core.result.StatementResult

/**
 * Base implementation of a [BlockingPreparedQueryBatch]. Delegates executing the queries to the
 * database specific implementation.
 */
abstract class BaseBlockingPreparedQueryBatch<C : BlockingConnection>(
    protected var connection: C?,
) : BlockingPreparedQueryBatch {
    protected val queries: MutableList<BlockingPreparedQuery> = mutableListOf()

    /**
     * Implementation specific method to execute and aggregate the results returned from the
     * database server into a single [StatementResult].
     */
    protected abstract fun vendorExecuteQueriesAggregating(): StatementResult

    /**
     * Execute all provided queries into a single [StatementResult] and allow for processing each
     * [io.github.clasicrando.kdbc.core.result.QueryResult] returned.
     */
    final override fun executeQueries(): StatementResult {
        checkNotNull(connection) { "QueryBatch already released its Connection" }
        if (queries.isEmpty()) {
            return StatementResult(emptyList())
        }
        return vendorExecuteQueriesAggregating()
    }

    /**
     * Create a new [BlockingPreparedQuery] using the provided [query] [String] and add it to the
     * queries contained within this batch of queries. Returns the newly created
     * [BlockingPreparedQuery] so parameters can be added if needed.
     *
     * @throws IllegalStateException if the batch has already been released
     */
    final override fun addPreparedQuery(query: String): BlockingPreparedQuery {
        checkNotNull(connection) { "QueryBatch already released its Connection" }
        val result = connection!!.createPreparedQuery(query)
        queries += result
        return result
    }

    override fun release() {
        connection = null
    }
}
