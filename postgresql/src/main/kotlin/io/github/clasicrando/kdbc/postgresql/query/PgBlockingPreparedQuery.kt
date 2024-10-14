package io.github.clasicrando.kdbc.postgresql.query

import io.github.clasicrando.kdbc.core.query.BaseBlockingPreparedQuery
import io.github.clasicrando.kdbc.core.result.StatementResult
import io.github.clasicrando.kdbc.postgresql.connection.PgBlockingConnection

/**
 * Postgresql implementation of a [io.github.clasicrando.kdbc.core.query.BlockingPreparedQuery]
 * where the [sql] query is executed using the extended query protocol with the [parameters] provided.
 */
internal class PgBlockingPreparedQuery(
    connection: PgBlockingConnection,
    sql: String,
) : BaseBlockingPreparedQuery<PgBlockingConnection>(connection = connection, sql = sql) {
    override fun vendorExecuteQuery(): StatementResult {
        return connection!!.sendExtendedQuery(sql, innerParameters)
    }
}
