package io.github.clasicrando.kdbc.core.query

import io.github.clasicrando.kdbc.core.result.StatementResult

/**
 * API to perform zero or more queries against a database
 * [io.github.clasicrando.kdbc.core.connection.Connection] where each query is a
 * [PreparedQuery]. Although this API is generally used for executing the same query with
 * different parameter values in each query, it is not limited to a single query string.
 */
interface PreparedQueryBatch : AutoCloseable {
    /**
     * Add a new [PreparedQuery] to execute in this batch. Returns the
     * [PreparedQuery] so parameters can be bound.
     */
    fun addPreparedQuery(query: String): PreparedQuery

    /** Execute all queries and aggregate into a single [StatementResult] for processing */
    suspend fun executeQueries(): StatementResult
}
