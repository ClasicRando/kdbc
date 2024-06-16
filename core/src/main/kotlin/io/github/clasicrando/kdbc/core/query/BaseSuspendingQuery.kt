package io.github.clasicrando.kdbc.core.query

import io.github.clasicrando.kdbc.core.connection.SuspendingConnection
import io.github.clasicrando.kdbc.core.result.StatementResult

/**
 * Base implementation of [SuspendingQuery]. Delegates the actual statement execution to the
 * database driver specified by [C].
 */
abstract class BaseSuspendingQuery<C: SuspendingConnection>(
    /** Reference to the [SuspendingConnection] that backs this [SuspendingQuery] */
    protected var connection: C?,
    final override val sql: String,
) : SuspendingQuery {
    /**
     * Database specific method to execute a [SuspendingQuery]. Assume that if this method is
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
