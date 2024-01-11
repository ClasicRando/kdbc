package com.github.clasicrando.common.pool

import com.github.clasicrando.common.connection.Connection
import kotlinx.coroutines.CoroutineScope

/** Provides the basis for how different database vendors create and validate [Connection]s */
interface ConnectionProvider {
    /**
     * Create a new connection for the implementor's database. Uses the [scope] to enable
     * cancelling of the created connection when the parent scope is cancelled.
     */
    suspend fun create(scope: CoroutineScope): Connection
    /** Validate that a [Connection] can be safely returned to a connection pool */
    suspend fun validate(item: Connection): Boolean
}
