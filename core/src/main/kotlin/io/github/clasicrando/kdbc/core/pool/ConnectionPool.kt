package io.github.clasicrando.kdbc.core.pool

import io.github.clasicrando.kdbc.core.connection.Connection
import io.github.clasicrando.kdbc.core.use
import kotlinx.coroutines.CoroutineScope

/**
 * Non-blocking pool of connections. Allows for acquiring of new connections and returning of
 * connections no longer needed.
 */
interface ConnectionPool<C : Connection> : CoroutineScope {
    /**
     * Attempt to acquire a [Connection] from the pool, suspending until a
     * [Connection] is available if the pool has been exhausted or waiting for the time
     * specified by [PoolOptions.acquireTimeout]. Make sure to return acquired
     * [Connection]s by calling [Connection.close] to return the
     * [Connection] to the pool.
     *
     * @throws AcquireTimeout if the waiting for the next available [Connection] exceeds
     * the [PoolOptions.acquireTimeout] value specified
     */
    suspend fun acquire(): C

    /**
     * Method to allow for returning of a [Connection] to the pool. THIS SHOULD ONLY BE
     * USED INTERNALLY and is called implicitly when a pool [Connection] is closed.
     * Returns true if the [Connection] was actually part of the pool and was returned.
     * Otherwise, return false.
     */
    suspend fun giveBack(connection: C): Boolean

    /**
     * Initialize resources within the pool. This involves validating the connection options
     * given to the pool can create valid connections and the pool is pre-populated with the
     * desired number of minimum connections required.
     */
    suspend fun initialize(): Boolean

    /** Close the connection pool and all connections that are associated with the pool */
    suspend fun close()
}

suspend inline fun <C : Connection, R> ConnectionPool<C>.useConnection(block: (C) -> R): R {
    return this.acquire().use(block)
}
