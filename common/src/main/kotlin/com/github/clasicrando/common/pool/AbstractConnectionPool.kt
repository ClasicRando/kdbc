package com.github.clasicrando.common.pool

import com.github.clasicrando.common.atomic.AtomicMutableMap
import com.github.clasicrando.common.connection.Connection
import io.github.oshai.kotlinlogging.KotlinLogging
import io.ktor.network.selector.SelectorManager
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.DelicateCoroutinesApi
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.isActive
import kotlinx.coroutines.job
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeout
import kotlinx.uuid.UUID
import kotlin.coroutines.CoroutineContext

private val logger = KotlinLogging.logger {}

/**
 * Default implementation of a [ConnectionPool], using a [Channel] to provide and buffer
 * [Connection] instances as needed. Uses the [poolOptions] and [provider] provided to set the pools
 * details and allow for creating/validating [Connection] instances.
 *
 * When the pool is created, an observer coroutine is launched to listen for requests for more
 * [Connection]s, creating new [Connection]s if the pool has not been exhausted. If the pool is
 * exhausted, no new connections will be made for the lifetime of the pool. Upon pool
 * initialization, the connections Channel is pre-populated with the [PoolOptions.minConnections]
 * value specified.
 *
 * TODO
 * - look into an algorithm to close connections after a certain duration stored within the
 * [connections] channel, down to the [PoolOptions.minConnections] threshold
 */
abstract class AbstractConnectionPool<C : Connection>(
    private val poolOptions: PoolOptions,
    private val provider: ConnectionProvider<C>,
) : ConnectionPool<C> {
    private val connections = Channel<C>(capacity = poolOptions.maxConnections)
    private val connectionIds: MutableMap<UUID, C> = AtomicMutableMap()
    private val connectionNeeded = Channel<Unit>(capacity = Channel.BUFFERED)

    final override val coroutineContext: CoroutineContext
        get() = SupervisorJob(
            parent = poolOptions.parentScope?.coroutineContext?.job,
        ) + poolOptions.coroutineDispatcher

    final override val selectorManager = SelectorManager(dispatcher = coroutineContext)

    private val initialized = CompletableDeferred<Boolean>(
        parent = selectorManager.coroutineContext.job,
    )

    /**
     * Create a new connection using the pool's [provider], set the connection's pool reference,
     * add the [Connection.connectionId] to the [connectionIds] set and send the [Connection] to
     * [connections] channel
     */
    private suspend fun addNewConnection() {
        val connection = provider.create(this@AbstractConnectionPool)
        connectionIds[connection.connectionId] = connection
        connections.send(connection)
    }

    private val observerJob: Job = launch {
        try {
            val connection = provider.create(this@AbstractConnectionPool)
            val isValid = provider.validate(connection)
            initialized.complete(isValid)
        } catch (ex: Throwable) {
            logger.atError {
                message = "Could not create the initial connection needed to validate the pool"
                cause = ex
            }
            initialized.completeExceptionally(ex)
        }

        for (i in 1..poolOptions.minConnections) {
            connectionNeeded.send(Unit)
        }

        var cause: Throwable? = null
        try {
            while (isActive) {
                connectionNeeded.receive()
                if (isExhausted) {
                    continue
                }
                addNewConnection()
            }
        } catch (ex: CancellationException) {
            cause = ex
        } catch (ex: Throwable) {
            this.cancel(message = "Error creating new connection", cause = ex)
            cause = ex
            throw ex
        } finally {
            connections.close(cause = cause)
            connectionNeeded.close(cause = cause)
            logger.atError {
                message = "Exiting pool observer"
            }
        }
    }

    abstract suspend fun disposeConnection(connection: C)

    /**
     * Invalidate a [Connection] from the pool by launching a coroutine to move the
     * [Connection] out of the pool's resources and references. This means, removing the
     * [Connection.connectionId] out of the [connectionIds] set, removing the reference to the pool
     * in the [Connection] and closing the actual [Connection]. This coroutine will never fail.
     */
    private fun invalidateConnection(connection: C) = launch {
        var connectionId: UUID? = null
        try {
            connectionId = connection.connectionId
            connectionIds.remove(connectionId)
            logger.atTrace {
                message = "Invalidating connection id = {id}"
                payload = mapOf("id" to connectionId)
            }
            disposeConnection(connection)
            connectionNeeded.send(Unit)
        } catch (ex: Throwable) {
            logger.atError {
                cause = ex
                message = "Error while closing invalid connection, '{connectionId}'"
                payload = connectionId?.let { mapOf("connectionId" to it) }
            }
        }
    }

    private suspend fun acquireConnection(): C {
        while (true) {
            connectionNeeded.send(Unit)
            val connection = connections.receive()
            if (provider.validate(connection)) {
                return connection
            }
            invalidateConnection(connection)
        }
    }

    override suspend fun acquire(): C {
        if (poolOptions.acquireTimeout.isInfinite()) {
            return acquireConnection()
        }
        return withTimeout(poolOptions.acquireTimeout) {
            acquireConnection()
        }
    }

    override val isExhausted: Boolean get() = connectionIds.size == poolOptions.maxConnections

    internal fun hasConnection(poolConnection: C): Boolean {
        return connectionIds.contains(poolConnection.connectionId)
    }

    @OptIn(DelicateCoroutinesApi::class)
    override suspend fun giveBack(connection: C): Boolean {
        if (!hasConnection(connection)) {
            return false
        }
        if (!provider.validate(connection) || connections.isClosedForSend) {
            invalidateConnection(connection)
            return true
        }
        connections.send(connection)
        return true
    }

    override suspend fun waitForValidation(): Boolean {
        return initialized.await()
    }

    override suspend fun close() {
        observerJob.cancelAndJoin()
        connections.close()
        connectionNeeded.close()
        for (connection in connectionIds.values) {
            connection.close()
        }
        withContext(Dispatchers.IO) {
            selectorManager.close()
        }
        cancel()
    }
}