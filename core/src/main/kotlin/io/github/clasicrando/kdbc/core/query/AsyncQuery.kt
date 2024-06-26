package io.github.clasicrando.kdbc.core.query

import io.github.clasicrando.kdbc.core.result.StatementResult

/**
 * API to perform a single query against a database
 * [io.github.clasicrando.kdbc.core.connection.AsyncConnection] with no parameters.
 *
 * This is the easiest way to execute queries against the database since you don't need to bind any
 * parameters to the query itself before executing. Although the API exists, it's generally
 * recommended to use a [AsyncPreparedQuery] since a [AsyncQuery] is not guaranteed to
 * create or cache a query plan for future calls of this same SQL query.
 */
interface AsyncQuery : AutoCloseable {
    /** SQL query to be executed */
    val sql: String

    /**
     * Simply execute the query and return the raw [StatementResult]
     *
     * @throws IllegalStateException if the query has already been closed
     */
    suspend fun execute(): StatementResult
}