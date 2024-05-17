package io.github.clasicrando.kdbc.core.connection

import io.github.clasicrando.kdbc.core.query.BlockingPreparedQuery
import io.github.clasicrando.kdbc.core.query.BlockingPreparedQueryBatch
import io.github.clasicrando.kdbc.core.query.BlockingQuery

private const val RESOURCE_TYPE = "BlockingConnection"

/**
 * A connection/session with a database. Each database vendor will provide these required
 * properties and methods to interact with any database implemented for this library.
 *
 * When you receive an instance of a [BlockingConnection], the underling connection has already been
 * established and the session has been prepared for immediate action.
 *
 * To keep the [BlockingConnection] resources from leaking, the recommended usage of
 * [BlockingConnection] instances is to utilize the [use] and [transaction] methods which always
 * clean up [BlockingConnection] resources before exiting. This does implicitly close the
 * connection so if you intend to hold a [BlockingConnection] for a long period of time (e.g.
 * outside the scope of a single method) you should find a way to always close the
 * [BlockingConnection].
 */
interface BlockingConnection : Connection {
    override val resourceType: String get() = RESOURCE_TYPE

    /**
     * Method called to close the connection and free any resources that are held by the
     * connection. Once this has been called, the [BlockingConnection] instance should not be used.
     */
    fun close()

    /**
     * Request that the database start a new transaction. This will fail if the
     * [BlockingConnection] is already within a transaction.
     */
    fun begin()

    /**
     * Commit the current transaction. This will fail if the [BlockingConnection] was not within a
     * transaction
     */
    fun commit()

    /**
     * Rollback the current transaction. This will fail if the [BlockingConnection] was not within
     * a transaction
     */
    fun rollback()

    /**
     * Create a new [BlockingQuery] for this [BlockingConnection] with the specified [query]
     * string. [BlockingQuery] instances are for SQL queries that do not accept parameters and
     * aren't executed frequently enough to require a precomputed query plan that is generated with
     * a [BlockingPreparedQuery]. This means that even if your query doesn't accept parameters, a
     * [BlockingPreparedQuery] is recommended when frequently executing a static query.
     */
    fun createQuery(query: String): BlockingQuery

    /**
     * Create a new [BlockingPreparedQuery] for this [BlockingConnection] with the specified
     * [query] string. [BlockingPreparedQuery] are for SQL queries that either accept parameters or
     * are executed frequently so a precomputed query plan is best.
     */
    fun createPreparedQuery(query: String): BlockingPreparedQuery

    /**
     * Create a new [BlockingPreparedQueryBatch] for this [BlockingConnection]. This allows
     * executing 1 or more [BlockingPreparedQuery] instances within a single batch of commands.
     * This is not guaranteed to improve performance but some databases provide optimized protocols
     * for sending multiple queries at the same time.
     */
    fun createPreparedQueryBatch(): BlockingPreparedQueryBatch
}

/**
 * Use a [BlockingConnection] within the specified [block], allowing for the [BlockingConnection]
 * to always call [BlockingConnection.close], even if the block throws an exception. This is
 * similar to the functionality that [AutoCloseable] provides where the resources are always
 * cleaned up before returning from the function. Note, this does not catch the exception, rather
 * it rethrows after cleaning up resources if an exception was thrown.
 */
inline fun <R, C : BlockingConnection> C.use(block: (C) -> R): R {
    var cause: Throwable? = null
    return try {
        block(this)
    } catch (ex: Throwable) {
        cause = ex
        throw ex
    } finally {
        try {
            close()
        } catch (ex: Throwable) {
            cause?.addSuppressed(ex)
        }
    }
}

/**
 * Use a [BlockingConnection] within the specified [block], allowing for the [BlockingConnection]
 * to always call [BlockingConnection.close], even if the block throws an exception. This is
 * similar to the functionality that [AutoCloseable] provides where the resources are always
 * cleaned up before returning from the function. Note, this does catch the exception and wraps
 * that is a [Result]. Otherwise, it returns a [Result] with the result of [block].
 */
inline fun <R, C : BlockingConnection> C.useCatching(block: (C) -> R): Result<R> {
    var cause: Throwable? = null
    return try {
        Result.success(block(this))
    } catch (ex: Throwable) {
        cause = ex
        Result.failure(ex)
    } finally {
        try {
            close()
        } catch (ex: Throwable) {
            cause?.addSuppressed(ex)
        }
    }
}

/**
 * Use a [BlockingConnection] within the scope of a transaction. This means an implicit
 * [BlockingConnection.begin] happens before [block] is called. If no exception is thrown then
 * [BlockingConnection.commit] is called, otherwise, [BlockingConnection.rollback] is called and
 * the original exception is rethrown. This all happens within a [BlockingConnection.use] block so
 * the resources are always cleaned up before returning.
 */
inline fun <R, C : BlockingConnection> C.transaction(block: (C) -> R): R = use {
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
 * Use a [BlockingConnection] within the scope of a transaction. This means an implicit
 * [BlockingConnection.begin] happens before [block] is called. If no exception is thrown then
 * [BlockingConnection.commit] is called, returning the outcome of [block] as a [Result].
 * Otherwise, [BlockingConnection.rollback] is called and the original exception is wrapped into a
 * [Result] and returned. This all happens within a [BlockingConnection.useCatching] block so the
 * resources are always cleaned up before returning and all other exceptions are caught and
 * returned as a [Result].
 */
inline fun <R, C : BlockingConnection> C.transactionCatching(
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
