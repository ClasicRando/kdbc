package io.github.clasicrando.kdbc.core.connection

import io.github.clasicrando.kdbc.core.AutoCloseableAsync
import io.github.clasicrando.kdbc.core.query.AsyncPreparedQuery
import io.github.clasicrando.kdbc.core.query.AsyncPreparedQueryBatch
import io.github.clasicrando.kdbc.core.query.AsyncQuery
import io.github.clasicrando.kdbc.core.use
import io.github.clasicrando.kdbc.core.useCatching

private const val RESOURCE_TYPE = "Connection"

/**
 * A connection/session with a database. Each database vendor will provide these required
 * properties and methods to interact with any database implemented for this library.
 *
 * When you receive an instance of a [AsyncConnection], the underling connection has already
 * been established and the session has been prepared for immediate action.
 *
 * To keep the [AsyncConnection] resources from leaking, the recommended usage of
 * [AsyncConnection] instances is to utilize the [use] and [transaction] methods which always
 * clean up [AsyncConnection] resources before exiting. This does implicitly close the
 * connection so if you intend to hold a [AsyncConnection] for a long period of time (e.g.
 * outside the scope of a single method) you should find a way to always close the
 * [AsyncConnection].
 */
interface AsyncConnection : Connection, AutoCloseableAsync {
    override val resourceType: String get() = RESOURCE_TYPE

    /**
     * Request that the database start a new transaction. This will fail if the
     * [AsyncConnection] is already within a transaction.
     */
    suspend fun begin()

    /**
     * Commit the current transaction. This will fail if the [AsyncConnection] was not within
     * a transaction
     */
    suspend fun commit()

    /**
     * Rollback the current transaction. This will fail if the [AsyncConnection] was not
     * within a transaction
     */
    suspend fun rollback()

    /**
     * Create a new [AsyncQuery] for this [AsyncConnection] with the specified [query]
     * string. [AsyncQuery] instances are for SQL queries that do not accept parameters and
     * aren't executed frequently enough to require a precomputed query plan that is generated with
     * a [AsyncPreparedQuery]. This means that even if your query doesn't accept parameters, a
     * [AsyncPreparedQuery] is recommended when frequently executing a static query.
     */
    fun createQuery(query: String): AsyncQuery

    /**
     * Create a new [AsyncPreparedQuery] for this [AsyncConnection] with the specified
     * [query] string. [AsyncPreparedQuery] are for SQL queries that either accept parameters
     * or are executed frequently so a precomputed query plan is best.
     */
    fun createPreparedQuery(query: String): AsyncPreparedQuery

    /**
     * Create a new [AsyncPreparedQueryBatch] for this [AsyncConnection]. This allows
     * executing 1 or more [AsyncPreparedQuery] instances within a single batch of commands.
     * This is not guaranteed to improve performance but some databases provide optimized protocols
     * for sending multiple queries at the same time.
     */
    fun createPreparedQueryBatch(): AsyncPreparedQueryBatch
}

/**
 * Use a [AsyncConnection] within the scope of a transaction. This means an implicit
 * [AsyncConnection.begin] happens before [block] is called. If no exception is thrown then
 * [AsyncConnection.commit] is called, otherwise, [AsyncConnection.rollback] is called
 * and the original exception is rethrown. This all happens within a [AutoCloseableAsync.use]
 * block so the resources are always cleaned up before returning.
 */
suspend inline fun <R, C : AsyncConnection> C.transaction(block: (C) -> R): R {
    return try {
        this.begin()
        val result = block(this)
        commit()
        result
    } catch (ex: Throwable) {
        rollback()
        throw ex
    }
}

/**
 * Use a [AsyncConnection] within the scope of a transaction. This means an implicit
 * [AsyncConnection.begin] happens before [block] is called. If no exception is thrown then
 * [AsyncConnection.commit] is called, returning the outcome of [block] as a [Result].
 * Otherwise, [AsyncConnection.rollback] is called and the original exception is wrapped into
 * a [Result] and returned. This all happens within a [AutoCloseableAsync.useCatching] block so
 * the resources are always cleaned up before returning and all other exceptions are caught and
 * returned as a [Result].
 */
suspend inline fun <R, C : AsyncConnection> C.transactionCatching(
    block: (C) -> R,
): Result<R> = runCatching {
    transaction(block)
}
