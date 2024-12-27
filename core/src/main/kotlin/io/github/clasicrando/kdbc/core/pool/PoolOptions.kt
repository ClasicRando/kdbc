package io.github.clasicrando.kdbc.core.pool

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.serialization.Serializable
import kotlin.time.Duration
import kotlin.time.DurationUnit
import kotlin.time.toDuration

/** Options when setting up a connection pool for any database vendor */
@Serializable
public data class PoolOptions(
    /**
     * Maximum number of connection instances held within the pool. Once this limit is reached, the
     * acquire method will suspend/block until connections are returned.
     */
    val maxConnections: Int = 10,
    /**
     * Minimum number of idle connections held within the pool. By default, this value is the same
     * as [maxConnections] to avoid unnecessary work to prune unused connections. If you want to
     * avoid too many idle connections to the database then you should lower the [maxConnections]
     * value. Generally, 10 connections isn't a large load for most long-lived async applications.
     */
    val minIdleConnections: Int = maxConnections,
    /**
     * Timeout value for acquiring a connection from the connection pool. This defaults to the max
     * wait time allowed but can be lowered if your application should abort waiting for a
     * connection if a call waits too long. Must be positive
     */
    val acquireTimeout: Duration = Duration.INFINITE,
    /**
     * [Duration] for how long a connection will stay idle within a connection pool before the pool
     * closes the connection (unless the [minIdleConnections] count is the current held count). Must
     * be positive.
     */
    val idleTimeout: Duration = 10.toDuration(DurationUnit.MINUTES),
    /**
     * [Duration] interval specifying how often idle connections are pinged to ensure they are not
     * timed out by forces outside this library's control (e.g. infrastructure or database rules
     * defining how long until an idle TCP connection is killed). Keep alive in this context has
     * nothing to do with the TCP protocol's `keepalive` property and does not impact the
     * [idleTimeout] property since that timeout is keep active even after a successful ping.
     */
    val idleKeepAliveInterval: Duration = 2.toDuration(DurationUnit.MINUTES),
    /**
     * True if connections should be validated before returning to the pool. By default, connections
     * are always checked before being acquired from the pool but checking before returning to the
     * pool ensures that broken connection does not lie in the pool taking up space.
     */
    val validateOnReturn: Boolean = true,
    /**
     * [Duration] for how long a connection should be used until retirement, regardless of the
     * current state of the connection. This is to avoid stale connections living too long and will
     * never impact an in-use connection since the check is performed when returning a connection to
     * the pool.
     *
     * If your infrastructure provider or database server itself imposes a similar limitation, this
     * value should be shorter to avoid possible conflicts.
     */
    val maxLifetime: Duration = 30.toDuration(DurationUnit.MINUTES),
    /** Optional parent scope that holds the connection pool's scope */
    val parentScope: CoroutineScope =
        CoroutineScope(
            Dispatchers.IO.limitedParallelism(Runtime.getRuntime().availableProcessors())
        ),
) {
    init {
        require(maxConnections > 0) { "Max connection count cannot be less than 1" }
        require(minIdleConnections >= 0) { "Min connection count cannot be less than 0" }
        require(acquireTimeout.isPositive()) { "acquireTimeout pool option must be positive" }
        require(idleTimeout.isPositive() && idleTimeout.isFinite()) {
            "idleTime pool option must be a positive finite value"
        }
        require(idleTimeout.inWholeSeconds > 10) { "Idle timeout must be greater than 10 seconds" }
        require(maxLifetime.inWholeSeconds > 30) {
            "Max Lifetime of a connection must be greater than 30 seconds"
        }
    }
}
