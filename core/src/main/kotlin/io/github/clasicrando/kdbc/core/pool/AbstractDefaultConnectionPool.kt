package io.github.clasicrando.kdbc.core.pool

import io.github.clasicrando.kdbc.core.connection.Connection
import io.github.clasicrando.kdbc.core.exceptions.KdbcException
import io.github.oshai.kotlinlogging.KotlinLogging
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.job
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import java.time.Instant
import kotlin.coroutines.CoroutineContext
import kotlin.time.Duration
import kotlin.time.DurationUnit
import kotlin.time.measureTimedValue
import kotlin.time.toDuration

private val logger = KotlinLogging.logger {}

/**
 * Default implementation of a [ConnectionPool], using a [Channel] to provide and buffer
 * [Connection] instances as needed. Uses the [poolOptions] and specified to set the pool's details
 * and allow for creating/validating [Connection] instances.
 */
public abstract class AbstractDefaultConnectionPool<C : Connection>(
    private val poolOptions: PoolOptions
) : ConnectionPool<C>, EntryCreator {
    final override val coroutineContext: CoroutineContext =
        SupervisorJob(parent = poolOptions.parentScope.coroutineContext.job)

    private val bag = ConcurrentBag<PoolEntry<C>>(this.coroutineContext, this)
    private var creationExecutor = ChannelExecutor(this, 1, poolOptions.maxConnections)
    private val cleaningDispatcher = Dispatchers.Default.limitedParallelism(1)

    final override suspend fun requestCreate(waiting: Int) {
        if (waiting > creationExecutor.requestCount) {
            creationExecutor.sendRequest(PoolEntryCreator())
        }
    }

    /** Create a new connection for the implementor's database */
    public abstract suspend fun create(): C

    /** Validate that a [Connection] can be safely returned to a connection pool */
    public open suspend fun validate(connection: C): Boolean {
        if (connection.isConnected && connection.inTransaction) {
            connection.rollback()
        }
        return connection.isConnected && !connection.inTransaction
    }

    /**
     * Database specific method to dispose of a [connection] when the connection is no longer valid
     * or the pool no longer needs to [connection].
     */
    public abstract suspend fun disposeConnection(connection: C)

    /** TODO */
    final override suspend fun acquire(): C {
        var timeout = poolOptions.acquireTimeout
        do {
            val (entry, duration) =
                measureTimedValue { bag.borrow(timeout = timeout) ?: throw AcquireTimeout() }

            if (
                entry.isEvicted ||
                    (hasExceededIdleTimeout(entry) && isInvalidConnection(entry.item))
            ) {
                invalidateConnection(entry)
                timeout -= duration
                continue
            }

            return entry.item
        } while (timeout > Duration.ZERO)

        throw AcquireTimeout()
    }

    /** TODO */
    private suspend fun invalidateConnection(entry: PoolEntry<C>) {
        try {
            bag.removeEntry(entry)
            logger.atTrace { message = "Invalidating entry = $entry" }
            disposeConnection(entry.item)
        } catch (ex: Exception) {
            logger.atError {
                cause = ex
                message = "Error while closing invalid connection, $entry"
            }
        }
    }

    /** TODO */
    internal fun hasConnection(poolConnection: C): Boolean {
        return bag.values.any { it.item.resourceId == poolConnection.resourceId }
    }

    final override suspend fun giveBack(connection: C): Boolean {
        val entry =
            bag.values.firstOrNull { it.item.resourceId == connection.resourceId } ?: return false
        if (poolOptions.validateOnReturn && isInvalidConnection(entry.item)) {
            invalidateConnection(entry)
            return true
        }

        entry.lastAccessed = Instant.now()
        bag.giveBack(entry)
        return true
    }

    override suspend fun close() {
        for (entry in bag.values) {
            invalidateConnection(entry)
        }
        logger.atTrace { message = "Canceling scope of connection pool" }
        cancel()
    }

    /** TODO */
    private suspend fun isInvalidConnection(connection: C): Boolean {
        try {
            if (!validate(connection)) {
                return true
            }
        } catch (ex: Exception) {
            logger.atTrace {
                this.cause = ex
                this.message = "Could not valid connection"
            }
            return true
        }
        return false
    }

    /** TODO */
    private fun hasExceededIdleTimeout(entry: PoolEntry<C>): Boolean {
        return Instant.now()
            .isAfter(entry.lastAccessed.plusMillis(poolOptions.idleTimeout.inWholeMilliseconds))
    }

    /** TODO */
    private inner class PoolEntryCreator : ChannelExecutor.Action {
        val maxBackoffDuration = 5.toDuration(DurationUnit.SECONDS)

        override suspend fun call() {
            var backoffDuration = 10.toDuration(DurationUnit.MILLISECONDS)
            var added = false
            while (shouldContinueCreating) {
                val entity = createNewConnection()
                if (entity == null) {
                    delay(backoffDuration)
                    backoffDuration = (backoffDuration * 2).coerceAtMost(maxBackoffDuration)
                    continue
                }
                added = true
                bag.addEntry(entity)
                delay(30)
                break
            }
        }

        val shouldContinueCreating: Boolean
            get() =
                bag.size < poolOptions.maxConnections &&
                    (bag.idleEntriesCount < poolOptions.minIdleConnections ||
                        bag.waitingCount > bag.idleEntriesCount)
    }

    /** TODO */
    private suspend fun createNewConnection(): PoolEntry<C>? {
        try {
            val connection = create()
            val entry = PoolEntry(connection)
            entry.setKeepAliveJob(keepAliveJob(entry, poolOptions.idleKeepAliveInterval))
            if (!poolOptions.maxLifetime.isInfinite()) {
                entry.setMaxLifetimeJob(maxLifetimeJob(entry, poolOptions.maxLifetime))
            }
            return entry
        } catch (ex: Exception) {
            logger.atDebug {
                message = "Failed to create a new connection"
                cause = ex
            }
        }
        return null
    }

    protected fun initializePool(): Unit = runBlocking {
        var initialConnection: PoolEntry<C>? = null
        try {
            initialConnection = createNewConnection()
            if (initialConnection == null || isInvalidConnection(initialConnection.item)) {
                throw KdbcException("Could not validate the initial connection to a pool")
            }
            bag.addEntry(entry = initialConnection)
        } catch (ex: KdbcException) {
            throw ex
        } catch (ex: Exception) {
            throw KdbcException("Could not validate the initial connection to a pool", ex)
        } finally {
            try {
                initialConnection?.item?.close()
            } catch (_: Exception) {}
        }
    }

    private fun CoroutineScope.maxLifetimeJob(entry: PoolEntry<C>, callbackTimeout: Duration): Job {
        return launch(cleaningDispatcher) {
            delay(callbackTimeout)
            if (softEvictConnection(entry, false)) {
                requestCreate(bag.waitingCount)
            }
        }
    }

    private fun CoroutineScope.keepAliveJob(entry: PoolEntry<C>, callbackTimeout: Duration): Job {
        return launch(cleaningDispatcher) {
            delay(callbackTimeout)
            while (isActive) {
                runCatching { keepAliveTask(entry) }
                delay(callbackTimeout)
            }
        }
    }

    private suspend fun keepAliveTask(entry: PoolEntry<C>) {
        if (!bag.reserveEntry(entry)) {
            return
        }

        if (isInvalidConnection(entry.item)) {
            softEvictConnection(entry, true)
            requestCreate(bag.waitingCount)
            return
        }

        bag.releaseEntry(entry)
    }

    private suspend fun softEvictConnection(entry: PoolEntry<C>, isEntryOwner: Boolean): Boolean {
        entry.isEvicted = true
        if (isEntryOwner || bag.reserveEntry(entry)) {
            invalidateConnection(entry)
            return true
        }
        return false
    }
}
