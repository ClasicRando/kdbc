package io.github.clasicrando.kdbc.core.pool

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.serialization.Serializable
import kotlin.time.Duration
import kotlin.time.DurationUnit
import kotlin.time.toDuration

/** Options when setting up a connection pool for any database vendor */
@Serializable
data class PoolOptions(
    /**
     * Maximum number of connection instances held within the pool. Once this limit is reached, the
     * acquire method will suspend/block until connections are returned.
     */
    val maxConnections: Int = 20,
    /**
     * Minimum number of connections held within the pool. When the pool is initialized, it will be
     * requested to create this number of connections and there will always be this many
     * connections at all times within the pool.
     */
    val minConnections: Int = 0,
    /**
     * Timeout value for acquiring a connection from the connection pool. This defaults to the max
     * wait time allowed but can be lowered if your application should abort waiting for a
     * connection if a call waits too long. Must be positive
     */
    val acquireTimeout: Duration = Duration.INFINITE,
    /**
     * [Duration] for how long a connection will stay idle within a connection pool before the pool
     * closes the connection (unless the [minConnections] count is the current held count). Must be
     * positive.
     *
     * This currently has no impact on the pool but will be used in future versions.
     */
    val idleTime: Duration = 1.toDuration(DurationUnit.MINUTES),
    /** Optional parent scope that holds the connection pool's scope */
    val parentScope: CoroutineScope = CoroutineScope(Dispatchers.IO),
) {
    init {
        require(acquireTimeout.isPositive()) {
            "acquireTimeout pool option must be positive"
        }
        require(idleTime.isPositive()) {
            "idleTime pool option must be positive"
        }
    }
}
