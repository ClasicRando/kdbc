package io.github.clasicrando.kdbc.core.connection

import io.github.clasicrando.kdbc.core.UniqueResourceId

/** Base properties of a database connection */
interface Connection : UniqueResourceId {
    /**
     * Returns true if the underlining connection is still active and false if the connection has
     * been closed or a fatal error has occurred causing the connection to be aborted
     */
    val isConnected: Boolean

    /** Returns true if the connection is currently within a transaction */
    val inTransaction: Boolean
}
