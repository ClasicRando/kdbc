package com.github.clasicrando.common.pool

import com.github.clasicrando.common.connection.Connection

/**
 * Extension of a [Connection] to allow referencing a [ConnectionPool] for returning to the
 * [Connection] to the referenced [pool] (if not null, otherwise it closes as normal)
 */
internal interface PoolConnection : Connection {
    /** [ConnectionPool] that owns this [Connection] (if any) */
    var pool: ConnectionPool?
}
