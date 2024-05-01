package io.github.clasicrando.kdbc.core.pool

import io.github.clasicrando.kdbc.core.connection.SuspendingConnection

/**
 * Provides the basis for how different database vendors create and validate
 * [SuspendingConnection]s
 */
interface SuspendingConnectionProvider<C : SuspendingConnection> {
    /**
     * Create a new connection for the implementor's database. Uses the [pool] to enable
     * cancelling of the created connection when the pool needs to be closed.
     */
    suspend fun create(pool: SuspendingConnectionPool<C>): C
    /** Validate that a [SuspendingConnection] can be safely returned to a connection pool */
    suspend fun validate(connection: C): Boolean
}
