package io.github.clasicrando.kdbc.core.query

import io.github.clasicrando.kdbc.core.connection.Connection
import io.github.clasicrando.kdbc.core.result.StatementResult

/**
 * Base implementation of [Query]. Delegates the actual statement execution to the database driver
 * specified by [C].
 */
abstract class BaseQuery<C: Connection>(
    /** Reference to the [Connection] that backs this [Query] */
    protected var connection: C?,
    final override val sql: String,
) : Query {
    /**
     * Database specific method to execute a [Query]. Assume that if this method is called, the
     * connection has been checked to be non-null.
     */
    abstract suspend fun vendorExecuteQuery(): StatementResult

    final override suspend fun execute(): StatementResult {
        checkNotNull(connection) { "Query already released its Connection" }
        return vendorExecuteQuery()
    }

    override fun release() {
        connection = null
    }
}
