package io.github.clasicrando.kdbc.core

import io.github.clasicrando.kdbc.core.connection.Connection

/** Main entry point for connection acquisition for a specific database vendor */
interface Database<C : Connection, O : Any> {
    /**
     * Create a new [Connection] (or reuse an existing connection if any are available)
     * using the supplied [connectOptions].
     */
    suspend fun connection(connectOptions: O): C
}
