package io.github.clasicrando.kdbc.core.pool

import io.github.clasicrando.kdbc.core.atomic.AtomicMutableMap
import io.github.clasicrando.kdbc.core.connection.Connection
import io.github.clasicrando.kdbc.core.exceptions.KdbcException
import io.github.oshai.kotlinlogging.KotlinLogging
import java.time.Instant
import kotlin.coroutines.CoroutineContext
import kotlin.time.DurationUnit
import kotlin.time.toDuration
import kotlin.uuid.Uuid
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.job
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.withTimeoutOrNull

private val logger = KotlinLogging.logger {}

/**
 * Default implementation of a [ConnectionPool], using a [Channel] to provide and buffer
 * [Connection] instances as needed. Uses the [poolOptions] and [provider] specified to set the
 * pool's details and allow for creating/validating [Connection] instances.
 *
 * Before using the pool, [initialize] must be called to verify the connection options can create
 * connections. Initialization also pre-populates the pool with the number of connections required
 * by [PoolOptions.minConnections].
 */
public abstract class AbstractDefaultConnectionPool<C : Connection>(
    private val poolOptions: PoolOptions,
    private val provider: ConnectionProvider<C>,
) : ConnectionPool<C> {
    private val idleCheckInterval = 30.toDuration(DurationUnit.SECONDS)
    private val connections = Channel<PoolEntry<C>>(capacity = Channel.UNLIMITED)
    private val connectionIds: MutableMap<Uuid, PoolEntry<C>> = AtomicMutableMap()
    private val connectionNeeded = Channel<CompletableDeferred<C?>>(capacity = Channel.BUFFERED)
    private val mutex = Mutex()

    final override val coroutineContext: CoroutineContext =
        SupervisorJob(parent = poolOptions.parentScope.coroutineContext.job)

    init {
        launch { idleConnectionPruner() }
        launch { idleConnectionKeepAlive() }
    }

    /**
     * Database specific method to dispose of a [connection] when the connection is no longer valid
     * or the pool no longer needs to [connection].
     */
    public abstract suspend fun disposeConnection(connection: C)

    /**
     * Attempt to get a connection from the pool. If a connection is available, it will be returned
     * without suspending. If a connection is not currently available, a [CompletableDeferred] is
     * put into the [connectionNeeded] channel, and it's completion is awaited. After resuming, the
     * available [Connection] is returned.
     *
     * @throws AcquireTimeout if the [CompletableDeferred] value does not complete before the
     *   [PoolOptions.acquireTimeout] duration is exceeded
     * @throws IllegalStateException if the [connections] channel is closed
     */
    override suspend fun acquire(): C {
        val entry = acquireConnection()
        if (entry != null) {
            if (validateConnection(entry.connection)) {
                return entry.connection
            }
            runCatching { invalidateConnection(entry) }
                .onFailure { ex ->
                    logger.atTrace {
                        this.cause = ex
                        this.message = "Failed closing invalid connection in pool"
                    }
                }
            return acquire()
        }
        val deferred = CompletableDeferred<C?>(parent = coroutineContext.job)
        connectionNeeded.send(deferred)
        val result =
            if (poolOptions.acquireTimeout.isInfinite()) {
                deferred.await()
            } else {
                withTimeoutOrNull(poolOptions.acquireTimeout) { deferred.await() }
            }
        if (result != null) {
            return result
        }
        deferred.complete(null)
        throw AcquireTimeout()
    }

    /**
     * Get the next available [Connection] from the [connections] channel. If the channel is empty
     * and the pool is not exhausted, [createNewConnection] is called to return a new connection.
     * This will return null if the channel is empty and the pool is exhausted.
     *
     * @throws IllegalStateException if the channel is closed
     */
    private suspend fun acquireConnection(): PoolEntry<C>? {
        val result = connections.tryReceive()
        when {
            result.isSuccess -> return result.getOrNull()
            result.isFailure ->
                return mutex.withLock {
                    if (!isExhausted) {
                        createNewConnection()
                    } else {
                        null
                    }
                }
            result.isClosed -> throw KdbcException("Connection channel for pool is closed")
        }
        return null
    }

    /**
     * Create a new connection using the pool's [provider], set the connection's pool reference, add
     * the [Connection.resourceId] to the [connectionIds] set and return the new connection
     */
    private suspend fun createNewConnection(): PoolEntry<C> {
        val connection = provider.create(this@AbstractDefaultConnectionPool)
        val entry = PoolEntry(connection)
        connectionIds[entry.connection.resourceId] = entry
        logger.atTrace {
            message =
                "Created new connection. Current pool size = ${connectionIds.size}. " +
                    "Max size = ${poolOptions.maxConnections}"
        }
        return entry
    }

    /**
     * Invalidate an [entry] from the pool by moving the [Connection] out of the pool's resources
     * and references. This means, removing the [Connection.resourceId] out of the [connectionIds]
     * set, removing the reference to the pool in the [Connection] and closing the actual
     * [Connection]. This action will only fail if the [logger] fails to log.
     */
    private suspend fun invalidateConnection(entry: PoolEntry<C>) {
        var connectionId: Uuid? = null
        try {
            connectionId = entry.connection.resourceId
            connectionIds.remove(connectionId)
            logger.atTrace { message = "Invalidating connection id = $connectionId" }
            disposeConnection(entry.connection)
        } catch (ex: Exception) {
            logger.atError {
                cause = ex
                message = "Error while closing invalid connection, '$connectionId'"
            }
        }
    }

    /**
     * Flag indicating if the pool of connections is exhausted (number of connections is use has
     * reached the cap on [PoolOptions.maxConnections]
     */
    private val isExhausted: Boolean
        get() = connectionIds.size >= poolOptions.maxConnections

    /** Checks the [connectionIds] lookup table for the [poolConnection]'s ID */
    internal fun hasConnection(poolConnection: C): Boolean {
        return connectionIds.contains(poolConnection.resourceId)
    }

    override suspend fun giveBack(connection: C): Boolean {
        val entry = connectionIds[connection.resourceId] ?: return false
        if (poolOptions.validateOnReturn && !validateConnection(entry.connection)) {
            mutex.withLock { invalidateConnection(entry) }
            return true
        }
        if (hasExceededLifetime(entry)) {
            mutex.withLock { invalidateConnection(entry) }
            return true
        }

        while (true) {
            val result = connectionNeeded.tryReceive()
            when {
                result.isSuccess -> {
                    if (result.getOrThrow().complete(entry.connection)) {
                        return true
                    }
                }
                result.isFailure -> break
                result.isClosed -> throw KdbcException("Connection channel for pool is closed")
            }
        }

        entry.lastAccessed = Instant.now()
        connections.send(entry)
        return true
    }

    override suspend fun initialize(): Boolean {
        var initialConnection: PoolEntry<C>? = null
        try {
            initialConnection = createNewConnection()
            if (!validateConnection(initialConnection.connection)) {
                return false
            }
        } catch (ex: Exception) {
            logger.atError {
                message = "Could not create the initial connection needed to validate the pool"
                cause = ex
            }
            return false
        } finally {
            try {
                initialConnection?.connection?.close()
            } catch (_: Throwable) {}
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
            connection.connection.close()
        }
        logger.atTrace { message = "Canceling scope of connection pool" }
        cancel()
    }

    private suspend fun validateConnection(connection: C): Boolean {
        return try {
            connection.isConnected && provider.validate(connection)
        } catch (ex: Exception) {
            logger.atTrace {
                this.cause = ex
                this.message = "Could not valid connection"
            }
            false
        }
    }

    private fun hasExceededIdleTimeout(entry: PoolEntry<C>): Boolean {
        return Instant.now()
            .isAfter(entry.lastAccessed.plusMillis(poolOptions.idleTimeout.inWholeMilliseconds))
    }

    private fun hasExceededLifetime(entry: PoolEntry<C>): Boolean {
        return Instant.now()
            .isAfter(entry.created.plusMillis(poolOptions.maxLifetime.inWholeMilliseconds))
    }

    private suspend fun CoroutineScope.idleConnectionPruner() {
        delay(100)
        val pulledConnections = mutableListOf<PoolEntry<C>>()
        while (isActive) {
            if (connectionIds.size <= poolOptions.minConnections) {
                delay(idleCheckInterval)
            }
            while (isActive && connectionIds.size > poolOptions.minConnections) {
                val result = connections.tryReceive()
                when {
                    result.isSuccess -> {
                        val entry = result.getOrNull() ?: continue
                        if (hasExceededIdleTimeout(entry)) {
                            invalidateConnection(entry)
                        } else {
                            pulledConnections.add(entry)
                        }
                    }
                    result.isClosed -> return
                    result.isFailure -> break
                }
            }
            pulledConnections.forEach { connections.send(it) }
            pulledConnections.clear()
        }
    }

    private suspend fun CoroutineScope.idleConnectionKeepAlive() {
        delay(100)
        val pulledConnections = mutableListOf<PoolEntry<C>>()
        while (isActive) {
            delay(poolOptions.idleKeepAliveInterval)
            while (isActive) {
                val result = connections.tryReceive()
                when {
                    result.isSuccess -> {
                        val entry = result.getOrNull() ?: continue
                        if (!entry.connection.isValid()) {
                            invalidateConnection(entry)
                            continue
                        }
                        pulledConnections.add(entry)
                    }
                    result.isClosed -> return
                    result.isFailure -> break
                }
            }
            pulledConnections.forEach { connections.send(it) }
            pulledConnections.clear()
        }
    }
}
