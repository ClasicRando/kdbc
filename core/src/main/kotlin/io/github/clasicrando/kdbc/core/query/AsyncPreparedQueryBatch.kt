package io.github.clasicrando.kdbc.core.query

import io.github.clasicrando.kdbc.core.result.StatementResult

/**
 * API to perform zero or more queries against a database
 * [io.github.clasicrando.kdbc.core.connection.AsyncConnection] where each query is a
 * [AsyncPreparedQuery]. Although this API is generally used for executing the same query with
 * different parameter values in each query, it is not limited to a single query string.
 */
interface AsyncPreparedQueryBatch : AutoCloseable {
    /**
     * Add a new [AsyncPreparedQuery] to execute in this batch. Returns the
     * [AsyncPreparedQuery] so parameters can be bound.
     */
    fun addPreparedQuery(query: String): AsyncPreparedQuery

    /** Execute all queries and aggregate into a single [StatementResult] for processing */
    suspend fun executeQueries(): StatementResult
}
