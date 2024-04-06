package com.github.clasicrando.common.pool

import com.github.clasicrando.common.connection.BlockingConnection

/**
 * Provides the basis for how different database vendors create and validate [BlockingConnection]s
 */
interface BlockingConnectionProvider<C : BlockingConnection> {
    /**
     * Create a new connection for the implementor's database. Uses the [pool] to enable
     * cancelling of the created connection when the pool needs to be closed.
     */
    fun create(pool: BlockingConnectionPool<C>): C
    /** Validate that a [BlockingConnection] can be safely returned to a connection pool */
    fun validate(connection: C): Boolean
}
