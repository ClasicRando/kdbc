package com.github.kdbc.core.connection

import com.github.kdbc.core.UniqueResourceId
import com.github.kdbc.core.query.DefaultQueryBatch
import com.github.kdbc.core.query.Query
import com.github.kdbc.core.query.QueryBatch
import com.github.kdbc.core.result.QueryResult
import com.github.kdbc.core.result.StatementResult

private const val RESOURCE_TYPE = "Connection"

/**
 * A connection/session with a database. Each database vendor will provide these required
 * properties and methods to interact with any database implemented for this library.
 *
 * When you receive an instance of a [Connection], the underling connection has already been
 * established and the session has been prepared for immediate action.
 *
 * To keep the [Connection] resources from leaking, the recommended usage of [Connection] instances
 * is to utilize the [use] and [transaction] methods which always clean up [Connection] resources
 * before exiting. This does implicitly close the connection so if you intend to hold a
 * [Connection] for a long period of time (e.g. outside the scope of a single method) you should
 * find a way to always close the [Connection].
 */
interface Connection : UniqueResourceId {
    override val resourceType: String get() = RESOURCE_TYPE

    /**
     * Returns true if the underlining connection is still active and false if the connection has
     * been closed or a fatal error has occurred causing the connection to be aborted
     */
    val isConnected: Boolean

    /**
     * Returns true if the connection is currently within a transaction. This is set to true after
     * [begin] is called and reverts to false if [commit] or [rollback] is called.
     */
    val inTransaction: Boolean

    /**
     * Method called to close the connection and free any resources that are held by the
     * connection. Once this has been called, the [Connection] instance should not be used.
     */
    suspend fun close()

    /**
     * Request that the database start a new transaction. This will fail if the [Connection] is
     * already within a transaction.
     */
    suspend fun begin()

    /**
     * Commit the current transaction. This will fail if the [Connection] was not within a
     * transaction
     */
    suspend fun commit()

    /**
     * Rollback the current transaction. This will fail if the [Connection] was not within a
     * transaction
     */
    suspend fun rollback()

    /**
     * Send a raw query with no parameters, returning a [StatementResult] containing zero or more
     * [QueryResult]s
     */
    suspend fun sendQuery(query: String): StatementResult

    /**
     * Send a prepared statement with [parameters], returning a [StatementResult] containing zero
     * or more [QueryResult]s
     */
    suspend fun sendPreparedStatement(query: String, parameters: List<Any?>): StatementResult

    /**
     * Manually release a prepared statement for the provided [query]. This will not error if the
     * query is not attached to a prepared statement.
     *
     * Only call this method if you are sure you need it.
     */
    suspend fun releasePreparedStatement(query: String)

    /** Create a new [Query] for this [Connection] with the specified [query] string */
    fun createQuery(query: String): Query = Query(query, this)

    /** Create a new [QueryBatch] for this [Connection] */
    fun createQueryBatch(): QueryBatch = DefaultQueryBatch(this)
}

/**
 * Use a [Connection] within the specified [block], allowing for the [Connection] to always
 * [Connection.close], even if the block throws an exception. This is similar to the functionality
 * that [AutoCloseable] provides where the resources are always cleaned up before returning from
 * the function. Note, this does not catch the exception, rather it rethrows after cleaning up
 * resources if an exception was thrown.
 */
suspend inline fun <R, C : Connection> C.use(block: (C) -> R): R {
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
 * Use a [Connection] within the specified [block], allowing for the [Connection] to always
 * [Connection.close], even if the block throws an exception. This is similar to the functionality
 * that [AutoCloseable] provides where the resources are always cleaned up before returning from
 * the function. Note, this does catch the exception and wraps that is a [Result]. Otherwise, it
 * returns a [Result] with the result of [block].
 */
suspend inline fun <R, C : Connection> C.useCatching(block: (C) -> R): Result<R> {
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
 * Use a [Connection] within the scope of a transaction. This means an implicit [Connection.begin]
 * happens before [block] is called. If no exception is thrown then [Connection.commit] is called,
 * otherwise, [Connection.rollback] is called and the original exception is rethrown. This all
 * happens within a [Connection.use] block so the resources are always cleaned up before returning.
 */
suspend inline fun <R, C : Connection> C.transaction(block: (C) -> R): R = use {
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
 * Use a [Connection] within the scope of a transaction. This means an implicit [Connection.begin]
 * happens before [block] is called. If no exception is thrown then [Connection.commit] is called,
 * returning the outcome of [block] as a [Result]. Otherwise, [Connection.rollback] is called and
 * the original exception is wrapped into a [Result] and returned. This all happens within a
 * [Connection.useCatching] block so the resources are always cleaned up before returning and all
 * other exceptions are caught and returned as a [Result].
 */
suspend inline fun <R, C : Connection> C.transactionCatching(
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
