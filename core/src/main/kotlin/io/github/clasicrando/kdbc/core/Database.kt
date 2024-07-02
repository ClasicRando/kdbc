package io.github.clasicrando.kdbc.core

import io.github.clasicrando.kdbc.core.connection.AsyncConnection
import io.github.clasicrando.kdbc.core.connection.BlockingConnection

/** Main entry point for connection acquisition for a specific database vendor */
interface Database<B : BlockingConnection, C: AsyncConnection, O : Any> {
    /**
     * Create a new [AsyncConnection] (or reuse an existing connection if any are available)
     * using the supplied [connectOptions].
     */
    suspend fun asyncConnection(connectOptions: O): C

    /**
     * Create a new [BlockingConnection] (or reuse an existing connection if any are available)
     * using the supplied [connectOptions].
     */
    fun blockingConnection(connectOptions: O): B
}
