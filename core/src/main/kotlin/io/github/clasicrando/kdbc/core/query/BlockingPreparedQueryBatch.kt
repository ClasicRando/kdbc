package io.github.clasicrando.kdbc.core.query

import io.github.clasicrando.kdbc.core.AutoRelease
import io.github.clasicrando.kdbc.core.result.StatementResult

/**
 * API to perform zero or more queries against a database
 * [io.github.clasicrando.kdbc.core.connection.BlockingConnection] where each query is a
 * [BlockingPreparedQuery]. Although this API is generally used for executing the same query with
 * different parameter values in each query, it is not limited to a single query string.
 */
interface BlockingPreparedQueryBatch : AutoRelease {
    /**
     * Add a new [BlockingPreparedQuery] to execute in this batch. Returns the
     * [BlockingPreparedQuery] so parameters can be bound.
     */
    fun addPreparedQuery(query: String): BlockingPreparedQuery

    /** Execute all queries and aggregate into a single [StatementResult] for processing */
    fun executeQueries(): StatementResult
}
