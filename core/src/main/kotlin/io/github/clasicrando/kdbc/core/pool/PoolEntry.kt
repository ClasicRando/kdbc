package io.github.clasicrando.kdbc.core.pool

import io.github.clasicrando.kdbc.core.connection.Connection
import java.time.Instant

/**
 * [Connection] entry within a connection pool. Tracks the creation [Instant] of the connection
 * and the [Instant] of the last time the pool was accessed. [lastAccessed] is only updated once the
 * pool is returned to the connection pool.
 */
internal class PoolEntry<C : Connection>(val connection: C) {
    val created: Instant = Instant.now()
    var lastAccessed: Instant = Instant.now()
}
