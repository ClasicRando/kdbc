package com.github.clasicrando.common.query

import com.github.clasicrando.common.AutoRelease
import com.github.clasicrando.common.connection.Connection
import com.github.clasicrando.common.result.QueryResult
import com.github.clasicrando.common.result.StatementResult

/**
 * A batch of [Query] instances can be executed and aggregated into a single [StatementResult] for
 * convenience since the basic [Query] API doesn't provide the ability to execute multiple queries
 * at once or extract multiple result blocks from queries.
 */
abstract class QueryBatch(protected var connection: Connection?) : AutoRelease {
    private val queries: MutableList<Query> = mutableListOf()

    /**
     * Implementation specific method to execute and aggregate the results returned from the
     * database server into a single [StatementResult].
     */
    protected abstract suspend fun executeQueriesAggregating(
        queries: List<Pair<String, List<Any?>>>
    ): StatementResult

    /**
     * Execute all provided queries into a single [StatementResult] and allow for processing each
     * [QueryResult] returned.
     */
    suspend fun executeQueries(): StatementResult {
        checkNotNull(connection) { "QueryBatch already released its Connection" }
        if (queries.isEmpty()) {
            return StatementResult(emptyList())
        }
        return executeQueriesAggregating(queries.map { it.query to it.parameters })
    }

    /**
     * Create a new [Query] using the provided [query] [String] and add it to the queries contained
     * within this batch of queries. Returns the newly created [Query] so parameters can be added
     * if needed.
     *
     * @throws IllegalStateException if the batch has already been released
     */
    fun addQuery(query: String): Query {
        checkNotNull(connection) { "QueryBatch already released its Connection" }
        val result = connection!!.createQuery(query)
        queries += result
        return result
    }

    override fun release() {
        connection = null
    }
}
