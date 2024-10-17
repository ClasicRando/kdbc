package io.github.clasicrando.kdbc.core.pool

import io.github.clasicrando.kdbc.core.atomic.AtomicMutableMap
import io.github.clasicrando.kdbc.core.config.Kdbc
import io.github.clasicrando.kdbc.core.connection.Connection
import io.github.clasicrando.kdbc.core.exceptions.CouldNotInitializeConnection
import io.github.oshai.kotlinlogging.KotlinLogging

private val logger = KotlinLogging.logger {}

/**
 * Manager of [Connection] and [ConnectionPool] instances for a given database
 * vendor. This type holds a collection of [ConnectionPool] instances per connect options
 * provided. Then each vendor's [Connection] will have a static method that calls this
 * [acquireConnection] to fetch the connect option's [ConnectionPool] and acquire a
 * [Connection] from that pool. If there is no [ConnectionPool] for the
 * supplied connect options, a new pool will be created, stored, and the first
 * [Connection] will be acquired.
 */
abstract class BasePoolManager<O : Any, C : Connection> {
    private val connectionPools: MutableMap<O, ConnectionPool<C>> = AtomicMutableMap()

    /**
     * Vendor specific method to create a [ConnectionPool] using the [options] provided
     */
    abstract fun createPool(options: O): ConnectionPool<C>

    /**
     * Acquire the next available connection for the specified [connectOptions]. This will call
     * [ConnectionPool.acquire] on the [ConnectionPool] associated with the
     * [connectOptions] provided. If a [ConnectionPool] has not already been created, a
     * new pool is added to the [ConnectionPool] collection before calling
     * [ConnectionPool.acquire].
     */
    suspend fun acquireConnection(connectOptions: O): C {
        return connectionPools.getOrPut(connectOptions) {
            val pool = createPool(connectOptions)
            val isValid = pool.initialize()
            if (!isValid) {
                throw CouldNotInitializeConnection(connectOptions.toString())
            }
            logger.at(Kdbc.detailedLogging) {
                message = "Created a new connection pool for $connectOptions"
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
