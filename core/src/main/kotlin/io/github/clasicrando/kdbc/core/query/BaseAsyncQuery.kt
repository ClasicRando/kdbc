package io.github.clasicrando.kdbc.core.query

import io.github.clasicrando.kdbc.core.connection.AsyncConnection
import io.github.clasicrando.kdbc.core.result.StatementResult

/**
 * Base implementation of [AsyncQuery]. Delegates the actual statement execution to the
 * database driver specified by [C].
 */
abstract class BaseAsyncQuery<C: AsyncConnection>(
    /** Reference to the [AsyncConnection] that backs this [AsyncQuery] */
    protected var connection: C?,
    final override val sql: String,
) : AsyncQuery {
    /**
     * Database specific method to execute a [AsyncQuery]. Assume that if this method is
     * called, the connection has been checked to be non-null.
     */
    abstract suspend fun vendorExecuteQuery(): StatementResult

    final override suspend fun execute(): StatementResult {
        checkNotNull(connection) { "Query already released its Connection" }
        return vendorExecuteQuery()
    }

    override fun close() {
        connection = null
    }
}
