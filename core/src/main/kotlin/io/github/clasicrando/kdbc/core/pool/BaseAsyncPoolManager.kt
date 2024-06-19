package io.github.clasicrando.kdbc.core.pool

import io.github.clasicrando.kdbc.core.atomic.AtomicMutableMap
import io.github.clasicrando.kdbc.core.connection.AsyncConnection
import io.github.clasicrando.kdbc.core.exceptions.CouldNotInitializeConnection
import io.github.oshai.kotlinlogging.KotlinLogging

private val logger = KotlinLogging.logger {}

/**
 * Manager of [AsyncConnection] and [AsyncConnectionPool] instances for a given database
 * vendor. This type holds a collection of [AsyncConnectionPool] instances per connect options
 * provided. Then each vendor's [AsyncConnection] will have a static method that calls this
 * [acquireConnection] to fetch the connect option's [AsyncConnectionPool] and acquire a
 * [AsyncConnection] from that pool. If there is no [AsyncConnectionPool] for the
 * supplied connect options, a new pool will be created, stored, and the first
 * [AsyncConnection] will be acquired.
 */
abstract class BaseAsyncPoolManager<O : Any, C : AsyncConnection> {
    private val connectionPools: MutableMap<O, AsyncConnectionPool<C>> = AtomicMutableMap()

    /**
     * Vendor specific method to create a [AsyncConnectionPool] using the [options] provided
     */
    abstract fun createPool(options: O): AsyncConnectionPool<C>

    /**
     * Acquire the next available connection for the specified [connectOptions]. This will call
     * [AsyncConnectionPool.acquire] on the [AsyncConnectionPool] associated with the
     * [connectOptions] provided. If a [AsyncConnectionPool] has not already been created, a
     * new pool is added to the [AsyncConnectionPool] collection before calling
     * [AsyncConnectionPool.acquire].
     */
    suspend fun acquireConnection(connectOptions: O): C {
        return connectionPools.getOrPut(connectOptions) {
            val pool = createPool(connectOptions)
            val isValid = pool.initialize()
            if (!isValid) {
                throw CouldNotInitializeConnection(connectOptions.toString())
            }
            logger.atTrace {
                message = "Created a new connection pool for {options}"
                payload = mapOf("options" to connectOptions)
            }
            pool
        }.acquire()
    }

    /** Close all pools associated with this pool manager */
    suspend fun closeAllPools() {
        for ((_, pool) in connectionPools) {
            pool.close()
        }
    }
}
