package io.github.clasicrando.kdbc.core.query

import io.github.clasicrando.kdbc.core.connection.BlockingConnection
import io.github.clasicrando.kdbc.core.result.StatementResult

/**
 * Base implementation of [BlockingQuery]. Delegates the actual statement execution to the database
 * driver specified by [C].
 */
abstract class BaseBlockingQuery<C: BlockingConnection>(
    /** Reference to the [BlockingConnection] that backs this [BlockingQuery] */
    protected var connection: C?,
    final override val sql: String,
) : BlockingQuery {
    /**
     * Database specific method to execute a [BlockingQuery]. Assume that if this method is called,
     * the connection has been checked to be non-null.
     */
    abstract fun vendorExecuteQuery(): StatementResult

    final override fun execute(): StatementResult {
        checkNotNull(connection) { "BlockingQuery already released its connection" }
        return vendorExecuteQuery()
    }

    override fun release() {
        connection = null
    }
}
