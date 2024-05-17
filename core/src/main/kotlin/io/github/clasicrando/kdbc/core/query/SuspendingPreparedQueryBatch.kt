package io.github.clasicrando.kdbc.core.query

import io.github.clasicrando.kdbc.core.AutoRelease
import io.github.clasicrando.kdbc.core.connection.SuspendingConnection
import io.github.clasicrando.kdbc.core.result.StatementResult

/**
 * API to perform zero or more queries against a database [SuspendingConnection] where each query
 * is a [SuspendingPreparedQuery]. Although this API is generally used for executing the same query
 * with different parameter values in each query, it is not limited to a single query string.
 */
interface SuspendingPreparedQueryBatch : AutoRelease {
    /**
     * Add a new [SuspendingPreparedQuery] to execute in this batch. Returns the
     * [SuspendingPreparedQuery] so parameters can be bound.
     */
    fun addPreparedQuery(query: String): SuspendingPreparedQuery

    /** Execute all queries and aggregate into a single [StatementResult] for processing */
    suspend fun executeQueries(): StatementResult
}
