package io.github.clasicrando.kdbc.core.connection

import io.github.clasicrando.kdbc.core.AutoCloseableAsync
import io.github.clasicrando.kdbc.core.query.SuspendingPreparedQuery
import io.github.clasicrando.kdbc.core.query.SuspendingPreparedQueryBatch
import io.github.clasicrando.kdbc.core.query.SuspendingQuery
import io.github.clasicrando.kdbc.core.use
import io.github.clasicrando.kdbc.core.useCatching

private const val RESOURCE_TYPE = "Connection"

/**
 * A connection/session with a database. Each database vendor will provide these required
 * properties and methods to interact with any database implemented for this library.
 *
 * When you receive an instance of a [SuspendingConnection], the underling connection has already
 * been established and the session has been prepared for immediate action.
 *
 * To keep the [SuspendingConnection] resources from leaking, the recommended usage of
 * [SuspendingConnection] instances is to utilize the [use] and [transaction] methods which always
 * clean up [SuspendingConnection] resources before exiting. This does implicitly close the
 * connection so if you intend to hold a [SuspendingConnection] for a long period of time (e.g.
 * outside the scope of a single method) you should find a way to always close the
 * [SuspendingConnection].
 */
interface SuspendingConnection : Connection, AutoCloseableAsync {
    override val resourceType: String get() = RESOURCE_TYPE

    /**
     * Request that the database start a new transaction. This will fail if the
     * [SuspendingConnection] is already within a transaction.
     */
    suspend fun begin()

    /**
     * Commit the current transaction. This will fail if the [SuspendingConnection] was not within
     * a transaction
     */
    suspend fun commit()

    /**
     * Rollback the current transaction. This will fail if the [SuspendingConnection] was not
     * within a transaction
     */
    suspend fun rollback()

    /**
     * Create a new [SuspendingQuery] for this [SuspendingConnection] with the specified [query]
     * string. [SuspendingQuery] instances are for SQL queries that do not accept parameters and
     * aren't executed frequently enough to require a precomputed query plan that is generated with
     * a [SuspendingPreparedQuery]. This means that even if your query doesn't accept parameters, a
     * [SuspendingPreparedQuery] is recommended when frequently executing a static query.
     */
    fun createQuery(query: String): SuspendingQuery

    /**
     * Create a new [SuspendingPreparedQuery] for this [SuspendingConnection] with the specified
     * [query] string. [SuspendingPreparedQuery] are for SQL queries that either accept parameters
     * or are executed frequently so a precomputed query plan is best.
     */
    fun createPreparedQuery(query: String): SuspendingPreparedQuery

    /**
     * Create a new [SuspendingPreparedQueryBatch] for this [SuspendingConnection]. This allows
     * executing 1 or more [SuspendingPreparedQuery] instances within a single batch of commands.
     * This is not guaranteed to improve performance but some databases provide optimized protocols
     * for sending multiple queries at the same time.
     */
    fun createPreparedQueryBatch(): SuspendingPreparedQueryBatch
}

/**
 * Use a [SuspendingConnection] within the scope of a transaction. This means an implicit
 * [SuspendingConnection.begin] happens before [block] is called. If no exception is thrown then
 * [SuspendingConnection.commit] is called, otherwise, [SuspendingConnection.rollback] is called
 * and the original exception is rethrown. This all happens within a [AutoCloseableAsync.use]
 * block so the resources are always cleaned up before returning.
 */
suspend inline fun <R, C : SuspendingConnection> C.transaction(block: (C) -> R): R = use {
    try {
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
 * Use a [SuspendingConnection] within the scope of a transaction. This means an implicit
 * [SuspendingConnection.begin] happens before [block] is called. If no exception is thrown then
 * [SuspendingConnection.commit] is called, returning the outcome of [block] as a [Result].
 * Otherwise, [SuspendingConnection.rollback] is called and the original exception is wrapped into
 * a [Result] and returned. This all happens within a [AutoCloseableAsync.useCatching] block so
 * the resources are always cleaned up before returning and all other exceptions are caught and
 * returned as a [Result].
 */
suspend inline fun <R, C : SuspendingConnection> C.transactionCatching(
    block: (C) -> R,
): Result<R> = useCatching {
    try {
        this.begin()
        val result = block(this)
        commit()
        result
    } catch (ex: Throwable) {
        rollback()
        throw ex
    }
}
