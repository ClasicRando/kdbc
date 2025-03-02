package io.github.clasicrando.kdbc.core.pool

import io.github.clasicrando.kdbc.core.connection.Connection
import io.github.clasicrando.kdbc.core.exceptions.KdbcException
import io.github.oshai.kotlinlogging.KotlinLogging
import java.time.Instant
import kotlin.coroutines.CoroutineContext
import kotlin.time.Duration
import kotlin.time.DurationUnit
import kotlin.time.measureTimedValue
import kotlin.time.toDuration
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
        SupervisorJob(parent = poolOptions.parentScope?.coroutineContext?.job) +
            Dispatchers.Default.limitedParallelism(Runtime.getRuntime().availableProcessors())

    private val bag = ConcurrentBag<PoolEntry<C>>(this)
    private var creationExecutor = ChannelExecutor(this, 1, poolOptions.maxConnections)
    private val cleaningDispatcher = Dispatchers.Default.limitedParallelism(1)
    private val poolEntryCreator = PoolEntryCreator()

    final override suspend fun requestCreate(waiting: Int) {
        if (waiting > creationExecutor.requestCount) {
            creationExecutor.sendRequest(poolEntryCreator)
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

    /**
     * Attempts to borrow a connection from this pool, waiting for a free connection if all are
     * currently in use. The method will throw [AcquireTimeout] if the [PoolOptions.acquireTimeout]
     * is exceeded while waiting for a connection
     */
    final override suspend fun acquire(): C {
        var timeout = poolOptions.acquireTimeout
        do {
            val (entry, duration) = measureTimedValue { bag.borrow(timeout = timeout) }
            if (entry == null) {
                throw AcquireTimeout()
            }

            // Final check to see if the borrowed entry has been evicted or exceeded timeout and is
            // invalid. If the check fails, invalidate that entry and try again
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

    /** Removed the [entry] from this pool and closes the underlining connection */
    private suspend fun invalidateConnection(entry: PoolEntry<C>) {
        try {
            bag.removeEntry(entry)
            val connection = entry.close()
            logger.atTrace { message = "Invalidating entry = $entry" }
            disposeConnection(connection)
        } catch (ex: Exception) {
            logger.atError {
                cause = ex
                message = "Error while closing invalid connection, $entry"
            }
        }
    }

    /** Returns true if the supplied [poolConnection] is found in the pool */
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
        creationExecutor.close()
        for (entry in bag.values) {
            invalidateConnection(entry)
        }
        cancel()
    }

    /**
     * Returns true if the [connection] cannot be validated or throws an exception while validating.
     * Otherwise, returns false.
     */
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

    /**
     * Returns true if the [entry] hasn't been accessed for a duration equal to the
     * [PoolOptions.idleTimeout]
     */
    private fun hasExceededIdleTimeout(entry: PoolEntry<C>): Boolean {
        return Instant.now()
            .isAfter(entry.lastAccessed.plusMillis(poolOptions.idleTimeout.inWholeMilliseconds))
    }

    /**
     * [ChannelExecutor.Action] that creates new [PoolEntry]s and adds them to the pool. If a new
     * entry fails to be created, there is an exponential backoff applied (starting at 10ms) for
     * each retry until a max duration of 5s.
     */
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

    /**
     * Create a new [PoolEntry] holding a [Connection]. Also initializes the keep alive job and max
     * lifetime jobs associated with the [Connection]. Returns the [PoolEntry] or null if any step
     * initializing the entry fails.
     */
    private suspend fun createNewConnection(): PoolEntry<C>? {
        try {
            val connection = create()
            val entry = PoolEntry.of(connection)
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

    /**
     * Initialize the first entry of the pool in a [runBlocking] action to allow for execution
     * during the object's constructor.
     *
     * The initial connection is created using [createNewConnection] and the result is checked to
     * ensure it's non-null and passes the [isInvalidConnection] test. If those succeed, the first
     * connection is added to the pool. Otherwise, a [KdbcException] is thrown describing the issue.
     */
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

    /**
     * Launch a coroutine that waits for a [callbackTimeout] and then soft evicts the specified
     * [entry] from the pool. If the eviction succeeds, a new entry is requested.
     */
    private fun CoroutineScope.maxLifetimeJob(entry: PoolEntry<C>, callbackTimeout: Duration): Job {
        return launch(cleaningDispatcher) {
            delay(callbackTimeout)
            if (softEvictConnection(entry, false)) {
                requestCreate(bag.waitingCount)
            }
        }
    }

    /**
     * Launch a coroutine that attempts to keep the [entry] alive every [callbackTimeout]. Once the
     * timeout is exceeded, [keepAliveTask] is run.
     */
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

        var didEvict = false
        try {
            if (isInvalidConnection(entry.item)) {
                softEvictConnection(entry, true)
                didEvict = true
                requestCreate(bag.waitingCount)
                return
            }
        } finally {
            if (!didEvict) {
                bag.releaseEntry(entry)
            }
        }
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
