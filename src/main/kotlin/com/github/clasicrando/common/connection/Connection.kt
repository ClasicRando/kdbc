package com.github.clasicrando.common.connection

import com.github.clasicrando.common.pool.ConnectionPool
import kotlinx.uuid.UUID

/**
 * A connection/session with a database. Each database vendor will provide these required
 * properties and methods to interact with any database implemented for this library. [Connection]
 * implementations extend the [Executor] interface to allow for query execution against the
 * [Connection] instance, however, unless you require transactional behaviour, the recommended
 * process is to use a [ConnectionPool][com.github.clasicrando.common.pool.ConnectionPool] instead
 * of a raw [Connection] for query execution.
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
interface Connection : Executor {
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
     * Unique identifier for the connection, utilized for logging to signify log messages as coming
     * from the same connection
     */
    val connectionId: UUID

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
     * Manually release a prepared statement for the provided [query]. This will not error if the
     * query is not attached to a prepared statement.
     *
     * Only call this method if you are sure you need it. Instead, use the `release` parameter in
     * a call of [Executor.sendPreparedStatement] to release a prepared statement immediately after
     * the query result finishes.
     */
    suspend fun releasePreparedStatement(query: String)
}

/**
 * Use a [Connection] within the specified [block], allowing for the [Connection] to always
 * [Connection.close], even if the block throws an exception. This is similar to the functionality
 * that [AutoCloseable] provides where the resources are always cleaned up before returning from
 * the function. Note, this does not catch the exception, rather it rethrows after cleaning up
 * resources if an exception was thrown.
 */
suspend inline fun <R, C : Connection> C.use(crossinline block: suspend (C) -> R): R {
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
suspend inline fun <R, C : Connection> C.useCatching(
    crossinline block: suspend (C) -> R,
): Result<R> {
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
suspend inline fun <R, C : Connection> C.transaction(
    crossinline block: suspend (C) -> R,
): R = use {
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
    crossinline block: suspend (C) -> R,
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
