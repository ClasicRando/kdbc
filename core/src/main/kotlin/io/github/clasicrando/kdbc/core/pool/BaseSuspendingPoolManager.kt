package io.github.clasicrando.kdbc.core.pool

import io.github.clasicrando.kdbc.core.atomic.AtomicMutableMap
import io.github.clasicrando.kdbc.core.connection.SuspendingConnection
import io.github.clasicrando.kdbc.core.exceptions.CouldNotInitializeConnection
import io.github.oshai.kotlinlogging.KotlinLogging

private val logger = KotlinLogging.logger {}

/**
 * Manager of [SuspendingConnection] and [SuspendingConnectionPool] instances for a given database
 * vendor. This type holds a collection of [SuspendingConnectionPool] instances per connect options
 * provided. Then each vendor's [SuspendingConnection] will have a static method that calls this
 * [acquireConnection] to fetch the connect option's [SuspendingConnectionPool] and acquire a
 * [SuspendingConnection] from that pool. If there is no [SuspendingConnectionPool] for the
 * supplied connect options, a new pool will be created, stored, and the first
 * [SuspendingConnection] will be acquired.
 */
abstract class BaseSuspendingPoolManager<O : Any, C : SuspendingConnection> {
    private val connectionPools: MutableMap<O, SuspendingConnectionPool<C>> = AtomicMutableMap()

    /**
     * Vendor specific method to create a [SuspendingConnectionPool] using the [options] provided
     */
    abstract fun createPool(options: O): SuspendingConnectionPool<C>

    /**
     * Acquire the next available connection for the specified [connectOptions]. This will call
     * [SuspendingConnectionPool.acquire] on the [SuspendingConnectionPool] associated with the
     * [connectOptions] provided. If a [SuspendingConnectionPool] has not already been created, a
     * new pool is added to the [SuspendingConnectionPool] collection before calling
     * [SuspendingConnectionPool.acquire].
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
