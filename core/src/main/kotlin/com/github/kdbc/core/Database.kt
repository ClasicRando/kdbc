package com.github.kdbc.core

import com.github.kdbc.core.connection.BlockingConnection
import com.github.kdbc.core.connection.Connection

/** Main entry point for connection acquisition for a specific database vendor */
interface Database<B : BlockingConnection, C: Connection, O : Any> {
    /**
     * Create a new [Connection] (or reuse an existing connection if any are available) using
     * the supplied [connectOptions].
     */
    suspend fun connection(connectOptions: O): C

    /**
     * Create a new [BlockingConnection] (or reuse an existing connection if any are available)
     * using the supplied [connectOptions].
     */
    fun blockingConnection(connectOptions: O): B
}
