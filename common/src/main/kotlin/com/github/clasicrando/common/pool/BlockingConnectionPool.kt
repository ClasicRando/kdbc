package com.github.clasicrando.common.pool

import com.github.clasicrando.common.connection.BlockingConnection

/**
 * Blocking pool of [BlockingConnection]s. Allows for acquiring of new connections and returning of
 * connections no longer needed.
 */
interface BlockingConnectionPool<C : BlockingConnection> {
    /**
     * Attempt to acquire a [BlockingConnection] from the pool, waiting until a
     * [BlockingConnection] is available if the pool has been exhausted or waiting for the time
     * specified by [PoolOptions.acquireTimeout]. Make sure to return acquired
     * [BlockingConnection]s by calling [BlockingConnection.close] to return the
     * [BlockingConnection] to the pool.
     *
     * @throws AcquireTimeout if the waiting for the next available [BlockingConnection] exceeds
     * the [PoolOptions.acquireTimeout] value specified
     */
    fun acquire(): C
    /**
     * Method to allow for returning of a [BlockingConnection] to the pool. THIS SHOULD ONLY BE
     * USED INTERNALLY and is called implicitly when a pool [BlockingConnection] is closed. Returns
     * true if the [BlockingConnection] was actually part of the pool and was returned. Otherwise,
     * returns false.
     */
    fun giveBack(connection: C): Boolean
    /**
     * Initialize resources within the pool. This involves validating the connection options
     * given to the pool can create valid connections and the pool is pre-populated with the
     * desired number of minimum connections required.
     */
    fun initialize(): Boolean
    /** Close the connection pool and all connections that are associated with the pool */
    fun close()
}
