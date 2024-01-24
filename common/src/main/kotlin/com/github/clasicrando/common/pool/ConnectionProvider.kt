package com.github.clasicrando.common.pool

import com.github.clasicrando.common.connection.Connection

/** Provides the basis for how different database vendors create and validate [Connection]s */
interface ConnectionProvider<C : Connection> {
    /**
     * Create a new connection for the implementor's database. Uses the [pool] to enable
     * cancelling of the created connection when the pool needs to be closed.
     */
    suspend fun create(pool: ConnectionPool<C>): C
    /** Validate that a [Connection] can be safely returned to a connection pool */
    suspend fun validate(connection: C): Boolean
}
