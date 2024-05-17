package io.github.clasicrando.kdbc.core

import io.github.clasicrando.kdbc.core.connection.BlockingConnection
import io.github.clasicrando.kdbc.core.connection.SuspendingConnection

/** Main entry point for connection acquisition for a specific database vendor */
interface Database<B : BlockingConnection, C: SuspendingConnection, O : Any> {
    /**
     * Create a new [SuspendingConnection] (or reuse an existing connection if any are available)
     * using the supplied [connectOptions].
     */
    suspend fun suspendingConnection(connectOptions: O): C

    /**
     * Create a new [BlockingConnection] (or reuse an existing connection if any are available)
     * using the supplied [connectOptions].
     */
    fun blockingConnection(connectOptions: O): B
}
