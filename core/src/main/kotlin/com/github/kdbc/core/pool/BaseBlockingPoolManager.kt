package com.github.kdbc.core.pool

import com.github.kdbc.core.atomic.AtomicMutableMap
import com.github.kdbc.core.connection.BlockingConnection
import com.github.kdbc.core.exceptions.CouldNotInitializeConnection
import io.github.oshai.kotlinlogging.KotlinLogging

private val logger = KotlinLogging.logger {}

/**
 * Manager of [BlockingConnection] and [BlockingConnectionPool] instances for a given database
 * vendor. This type holds a collection of [BlockingConnectionPool] instances per connect options
 * provided. Then each vendor's [BlockingConnection] will have a static method that calls this
 * [acquireConnection] to fetch the connect option's [BlockingConnectionPool] and acquire a
 * [BlockingConnection] from that pool. If there is no [BlockingConnectionPool] for the supplied
 * connect options, a new pool will be created, stored, and the first [BlockingConnection] will be
 * acquired.
 */
abstract class BaseBlockingPoolManager<O : Any, C : BlockingConnection> {
    private val connectionPools: MutableMap<O, BlockingConnectionPool<C>> = AtomicMutableMap()

    /** Vendor specific method to create a [BlockingConnectionPool] using the [options] provided */
    abstract fun createPool(options: O): BlockingConnectionPool<C>

    /**
     * Acquire the next available connection for the specified [connectOptions]. This will call
     * [BlockingConnectionPool.acquire] on the [BlockingConnectionPool] associated with the
     * [connectOptions] provided. If a [BlockingConnectionPool] has not already been created, a new
     * pool is added to the [BlockingConnectionPool] collection before calling
     * [BlockingConnectionPool.acquire].
     */
    fun acquireConnection(connectOptions: O): C {
        return connectionPools.getOrPut(connectOptions) {
            val pool = createPool(connectOptions)
            val isValid = pool.initialize()
            if (!isValid) {
                throw CouldNotInitializeConnection(connectOptions.toString())
            }
            logger.atTrace {
                message = "Created a new blocking connection pool for {options}"
                payload = mapOf("options" to connectOptions)
            }
            pool
        }.acquire()
    }

    /** Close all pools associated with this pool manager */
    fun closeAllPools() {
        for ((_, pool) in connectionPools) {
            pool.close()
        }
    }
}