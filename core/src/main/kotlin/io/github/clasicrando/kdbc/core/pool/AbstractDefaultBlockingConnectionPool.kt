package io.github.clasicrando.kdbc.core.pool

import io.github.clasicrando.kdbc.core.connection.BlockingConnection
import io.github.clasicrando.kdbc.core.exceptions.KdbcException
import io.github.oshai.kotlinlogging.KotlinLogging
import kotlinx.atomicfu.locks.reentrantLock
import kotlinx.atomicfu.locks.withLock
import kotlin.uuid.Uuid
import java.util.concurrent.BlockingDeque
import java.util.concurrent.CompletableFuture
import java.util.concurrent.LinkedBlockingDeque
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException

private val logger = KotlinLogging.logger {}

/**
 * Default implementation of a [BlockingConnectionPool], using a [BlockingDeque] to provide and
 * buffer [BlockingConnection] instances as needed. Uses the [poolOptions] and [provider] specified
 * to set the pool's details and allow for creating/validating [BlockingConnection] instances.
 *
 * Before using the pool, [initialize] must be called to verify the connection options can create
 * connections. Initialization also pre-populates the pool with the number of connections required
 * by [PoolOptions.minConnections].
 *
 * TODO
 * - look into an algorithm to close connections after a certain duration stored within the
 * [connections] dequeue, down to the [PoolOptions.minConnections] threshold
 */
abstract class AbstractDefaultBlockingConnectionPool<C : BlockingConnection>(
    private val poolOptions: PoolOptions,
    private val provider: BlockingConnectionProvider<C>,
) : BlockingConnectionPool<C> {
    private val connections: BlockingDeque<C> = LinkedBlockingDeque()
    private val connectionIds: MutableMap<Uuid, C> = mutableMapOf()
    private val connectionNeeded: BlockingDeque<CompletableFuture<C>> = LinkedBlockingDeque()
    private val lock = reentrantLock()

    /**
     * Create a new connection using the pool's [provider], set the connection's pool reference,
     * add the [BlockingConnection.resourceId] to the [connectionIds] set and return the new
     * connection
     */
    private fun createNewConnection(): C {
        val connection = provider.create(this@AbstractDefaultBlockingConnectionPool)
        connectionIds[connection.resourceId] = connection
        logger.atTrace {
            message = "Created new connection. Current pool size = ${connectionIds.size}. " +
                    "Max size = ${poolOptions.maxConnections}"
        }
        return connection
    }

    /**
     * Database specific method to dispose of a [connection] when the connection is no longer valid
     * or the pool no longer needs to [connection].
     */
    abstract fun disposeConnection(connection: C)

    /**
     * Invalidate a [connection] from the pool by moving the [BlockingConnection] out of the pool's
     * resources and references. This means, removing the [BlockingConnection.resourceId] out of
     * the [connectionIds] set, removing the reference to the pool in the [BlockingConnection] and
     * closing the actual [BlockingConnection]. This action will only fail if the [logger] fails to
     * log.
     */
    private fun invalidateConnection(connection: C) {
        var connectionId: Uuid? = null
        try {
            connectionId = connection.resourceId
            lock.withLock { connectionIds.remove(connectionId) }
            logger.atTrace {
                message = "Invalidating connection id = $connectionId"
            }
            disposeConnection(connection)
        } catch (ex: Throwable) {
            logger.atError {
                cause = ex
                message = "Error while closing invalid connection, '$connectionId'"
            }
        }
    }

    /**
     * Get the next available [BlockingConnection] from the [connections] queue. If the queue is
     * empty and the pool is not exhausted, [createNewConnection] is called to return a new
     * connection. This will return null if the channel is empty and the pool is exhausted.
     */
    private fun acquireConnection(): C? {
        val result = connections.poll()
        if (result != null) {
            return result
        }
        lock.withLock {
            if (!isExhausted) {
                return createNewConnection()
            }
        }
        return null
    }

    /**
     * Attempt to get a connection from the pool. If a connection is available, it will be returned
     * without waiting. If a connection is not currently available, a [CompletableFuture] is put
     * into the [connectionNeeded] queue, and it's completion is polled. After the future
     * completes, the available [BlockingConnection] is returned.
     *
     * @throws AcquireTimeout if the [CompletableFuture] value does not complete before the
     * [PoolOptions.acquireTimeout] duration is exceeded
     */
    override fun acquire(): C {
        val connection = acquireConnection()
        if (connection != null) {
            return connection
        }
        val deferred = CompletableFuture<C>()
        connectionNeeded.put(deferred)
        val result = try {
            deferred.get(
                poolOptions.acquireTimeout.inWholeMilliseconds,
                TimeUnit.MILLISECONDS,
            )
        } catch (ex: TimeoutException) {
            throw AcquireTimeout()
        }
        if (result != null) {
            return result
        }
        deferred.complete(null)
        throw AcquireTimeout()
    }

    /**
     * Flag indicating if the pool of connections is exhausted (number of connections is use has
     * reached the cap on [PoolOptions.maxConnections]
     */
    private val isExhausted: Boolean get() = connectionIds.size >= poolOptions.maxConnections

    /** Checks the [connectionIds] lookup table for the [poolConnection]'s ID */
    internal fun hasConnection(poolConnection: C): Boolean {
        return lock.withLock {
            connectionIds.contains(poolConnection.resourceId)
        }
    }

    override fun giveBack(connection: C): Boolean {
        if (!hasConnection(connection)) {
            return false
        }
        if (!provider.validate(connection)) {
            invalidateConnection(connection)
            return true
        }
        while (true) {
            val result = connectionNeeded.poll() ?: break
            if (result.complete(connection)) {
                return true
            }
        }

        connections.put(connection)
        return true
    }

    override fun initialize(): Boolean {
        var initialConnection: C? = null
        try {
            initialConnection = createNewConnection()
            if (!provider.validate(initialConnection)) {
                return false
            }
        } catch (ex: Throwable) {
            logger.atError {
                message = "Could not create the initial connection needed to validate the pool"
                cause = ex
            }
            return false
        } finally {
            try {
                initialConnection?.close()
            } catch (ignored: Throwable) {}
        }
        for (i in 1..<poolOptions.minConnections) {
            connections.put(createNewConnection())
        }
        return true
    }

    override fun close() {
        while (true) {
            val result = connectionNeeded.poll() ?: break
            val error = KdbcException("Pool closed while connections are still being requested")
            result.completeExceptionally(error)
        }
        for (connection in connectionIds.values) {
            connection.close()
        }
    }
}