package com.github.kdbc.core.pool

import kotlinx.coroutines.CoroutineScope
import kotlinx.serialization.Serializable
import kotlin.time.Duration
import kotlin.time.DurationUnit
import kotlin.time.toDuration

/** Options when setting up a [ConnectionPool] */
@Serializable
data class PoolOptions(
    /**
     * Maximum number of [Connection][com.github.kdbc.core.connection.Connection]
     * instances held within the pool. Once this limit is reached, the [ConnectionPool.acquire]
     * method will suspend until [Connection][com.github.kdbc.core.connection.Connection]s
     * are returned.
     */
    val maxConnections: Int = 20,
    /**
     * Minimum number of [Connection][com.github.kdbc.core.connection.Connection]
     * instances held within the pool. When the pool is initialized, it will be requested to create
     * this number of [Connection][com.github.kdbc.core.connection.Connection]s and there
     * will always be this many [Connection][com.github.kdbc.core.connection.Connection]s
     * at all times within the pool.
     */
    val minConnections: Int = 0,
    /**
     * Timeout value for acquiring a
     * [Connection][com.github.kdbc.core.connection.Connection] from the [ConnectionPool].
     * This defaults to the max wait time allowed but can be lowered if your application should
     * abort waiting for a connection if a call waits too long. Must be positive
     */
    val acquireTimeout: Duration = Duration.INFINITE,
    /**
     * [Duration] for how long a [Connection][com.github.kdbc.core.connection.Connection]
     * will stay idle within a [ConnectionPool] before the pool closes the
     * [Connection][com.github.kdbc.core.connection.Connection] (unless the
     * [minConnections] count is the current held count). Must be positive.
     *
     * This currently has no impact on the pool but will be used in future versions.
     */
    val idleTime: Duration = 1.toDuration(DurationUnit.MINUTES),
    /** Optional parent scope that holds the [ConnectionPool]s scope */
    val parentScope: CoroutineScope? = null,
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
