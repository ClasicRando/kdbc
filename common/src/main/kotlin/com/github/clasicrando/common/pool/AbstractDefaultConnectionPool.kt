package com.github.clasicrando.common.pool

import com.github.clasicrando.common.atomic.AtomicMutableMap
import com.github.clasicrando.common.connection.Connection
import io.github.oshai.kotlinlogging.KotlinLogging
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.job
import kotlinx.coroutines.withTimeoutOrNull
import kotlinx.uuid.UUID
import kotlin.coroutines.CoroutineContext

private val logger = KotlinLogging.logger {}

/**
 * Default implementation of a [ConnectionPool], using a [Channel] to provide and buffer
 * [Connection] instances as needed. Uses the [poolOptions] and [provider] specified to set the
 * pool's details and allow for creating/validating [Connection] instances.
 *
 * Before using the pool, [initialize] must be called to verify the connection options can create
 * connections. Initialization also pre-populates the pool with the number of connections required
 * by [PoolOptions.minConnections].
 *
 * TODO
 * - look into an algorithm to close connections after a certain duration stored within the
 * [connections] channel, down to the [PoolOptions.minConnections] threshold
 */
abstract class AbstractDefaultConnectionPool<C : Connection>(
    private val poolOptions: PoolOptions,
    private val provider: ConnectionProvider<C>,
) : ConnectionPool<C> {
    private val connections = Channel<C>(capacity = poolOptions.maxConnections)
    private val connectionIds: MutableMap<UUID, C> = AtomicMutableMap()
    private val connectionNeeded = Channel<CompletableDeferred<C?>>(capacity = Channel.BUFFERED)

    final override val coroutineContext: CoroutineContext = SupervisorJob(
        parent = poolOptions.parentScope?.coroutineContext?.job,
    )

    /**
     * Create a new connection using the pool's [provider], set the connection's pool reference,
     * add the [Connection.connectionId] to the [connectionIds] set and return the new connection
     */
    private suspend fun createNewConnection(): C {
        val connection = provider.create(this@AbstractDefaultConnectionPool)
        connectionIds[connection.connectionId] = connection
        logger.atTrace {
            message = "Created new connection. Current pool size = {count}. Max size = {max}"
            payload = mapOf(
                "count" to connectionIds.size,
                "max" to poolOptions.maxConnections,
            )
        }
        return connection
    }

    /**
     * Database specific method to dispose of a [connection] when the connection is no longer valid
     * or the pool no longer needs to [connection].
     */
    abstract suspend fun disposeConnection(connection: C)

    /**
     * Invalidate a [connection] from the pool by moving the [Connection] out of the pool's
     * resources and references. This means, removing the [Connection.connectionId] out of the
     * [connectionIds] set, removing the reference to the pool in the [Connection] and closing the
     * actual [Connection]. This action will only fail if the [logger] fails to log.
     */
    private suspend fun invalidateConnection(connection: C) {
        var connectionId: UUID? = null
        try {
            connectionId = connection.connectionId
            connectionIds.remove(connectionId)
            logger.atTrace {
                message = "Invalidating connection id = {id}"
                payload = mapOf("id" to connectionId)
            }
            disposeConnection(connection)
        } catch (ex: Throwable) {
            logger.atError {
                cause = ex
                message = "Error while closing invalid connection, '{connectionId}'"
                payload = connectionId?.let { mapOf("connectionId" to it) }
            }
        }
    }

    /**
     * Get the next available [Connection] from the [connections] channel. If the channel is empty
     * and the pool is not exhausted, [createNewConnection] is called to return a new connection.
     * This will return null if the channel is empty and the pool is exhausted.
     *
     * @throws IllegalStateException if the channel is closed
     */
    private suspend fun acquireConnection(): C? {
        val result = connections.tryReceive()
        when  {
            result.isSuccess -> return result.getOrThrow()
            result.isFailure && !isExhausted -> return createNewConnection()
            result.isClosed -> error("Connection channel for pool is closed")
        }
        return null
    }

    /**
     * Attempt to get a connection from the pool. If a connection is available, it will be returned
     * without suspending. If a connection is not currently available, a [CompletableDeferred] is
     * put into the [connectionNeeded] channel, and it's completion is awaited. After resuming,
     * the available [Connection] is returned.
     *
     * @throws AcquireTimeout if the [CompletableDeferred] value does not complete before the
     * [PoolOptions.acquireTimeout] duration is exceeded
     * @throws IllegalStateException if the [connections] channel is closed
     */
    override suspend fun acquire(): C {
        val connection = acquireConnection()
        if (connection != null) {
            return connection
        }
        val deferred = CompletableDeferred<C?>(parent = coroutineContext.job)
        connectionNeeded.send(deferred)
        val result = if (poolOptions.acquireTimeout.isInfinite()) {
            deferred.await()
        } else {
            withTimeoutOrNull(poolOptions.acquireTimeout) {
                deferred.await()
            }
        }
        if (result != null) {
            return result
        }
        deferred.complete(null)
        throw AcquireTimeout()
    }

    override val isExhausted: Boolean get() = connectionIds.size >= poolOptions.maxConnections

    /** Checks the [connectionIds] lookup table for the [poolConnection]'s ID */
    internal fun hasConnection(poolConnection: C): Boolean {
        return connectionIds.contains(poolConnection.connectionId)
    }

    override suspend fun giveBack(connection: C): Boolean {
        if (!hasConnection(connection)) {
            return false
        }
        if (!provider.validate(connection)) {
            invalidateConnection(connection)
            return true
        }
        while (true) {
            val result = connectionNeeded.tryReceive()
            when {
                result.isSuccess -> {
                    if (result.getOrThrow().complete(connection)) {
                        return true
                    }
                }
                result.isFailure -> break
                result.isClosed -> error("Connection channel for pool is closed")
            }
        }

        return connections.trySend(connection).isSuccess
    }

    override suspend fun initialize(): Boolean {
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
            connections.send(createNewConnection())
        }
        return true
    }

    override suspend fun close() {
        connections.close()
        while (true) {
            val result = connectionNeeded.tryReceive()
            when {
                result.isSuccess -> {
                    val error = Exception("Pool closed while connections are still being requested")
                    result.getOrNull()?.completeExceptionally(error)
                }
                result.isFailure || result.isClosed -> break
            }
        }
        connectionNeeded.close()
        for (connection in connectionIds.values) {
            connection.close()
        }
        cancel()
    }
}