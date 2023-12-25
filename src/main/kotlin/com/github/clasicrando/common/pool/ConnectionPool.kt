package com.github.clasicrando.common.pool

import com.github.clasicrando.common.connection.Connection
import com.github.clasicrando.common.connection.Executor
import com.github.clasicrando.common.connection.use
import kotlinx.coroutines.CoroutineScope

/**
 * Non-blocking pool of connections. Allows for acquiring of new connections and using the pool as
 * an [Executor] to avoid handling connections explicitly.
 */
interface ConnectionPool : Executor, CoroutineScope {
    /**
     * Flag indicating if the pool of connections is exhausted (number of connections is use has
     * reached the cap on [PoolOptions.maxConnections]
     */
    val isExhausted: Boolean
    /**
     * Attempt to acquire a [Connection] from the pool, suspending until a [Connection] is
     * available if the pool has been exhausted or waiting for the time specified by
     * [PoolOptions.acquireTimeout]. Make sure to return acquired [Connection]s by using the
     * [useConnection] extension method or calling [Connection.close] to return the [Connection] to
     * the pool.
     */
    suspend fun acquire(): Connection
    /**
     * Method to allow for returning of a [Connection] to the pool. THIS SHOULD ONLY BE USED
     * INTERNALLY and is called implicitly when a pool [Connection] is closed. Returns true if the
     * [Connection] was actually part of the pool and was returned. Otherwise, return false.
     */
    suspend fun giveBack(connection: Connection): Boolean
    /** Close the connection pool and all connections that are associated with the pool */
    suspend fun close()
}

/** Use a connection pool for a scoped duration, always closing even if an exception is thrown */
internal suspend inline fun <R> ConnectionPool.use(
    crossinline block: suspend (ConnectionPool) -> R
): R {
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
 * Acquire a new [Connection] from the [ConnectionPool] for usage within the [block], always
 * returning the connection to the pool when returning from the function.
 */
suspend inline fun <R> ConnectionPool.useConnection(
    crossinline block: suspend (Connection) -> R,
): R {
    return acquire().use(block)
}
