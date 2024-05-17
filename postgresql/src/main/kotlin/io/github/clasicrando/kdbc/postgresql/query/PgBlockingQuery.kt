package io.github.clasicrando.kdbc.postgresql.query

import io.github.clasicrando.kdbc.core.query.BaseBlockingQuery
import io.github.clasicrando.kdbc.core.query.BlockingQuery
import io.github.clasicrando.kdbc.core.result.StatementResult
import io.github.clasicrando.kdbc.postgresql.connection.PgBlockingConnection
import io.github.clasicrando.kdbc.postgresql.connection.PgConnectOptions

/**
 * Postgresql implementation of a [BlockingQuery] where the [sql] query is usually executed using
 * the simple query protocol unless the query text does not contain a semicolon and
 * [PgConnectOptions.useExtendedProtocolForSimpleQueries] is true (the default value).
 */
class PgBlockingQuery(
    connection: PgBlockingConnection,
    sql: String
) : BaseBlockingQuery<PgBlockingConnection>(sql = sql, connection = connection) {
    override fun vendorExecuteQuery(): StatementResult {
        return connection?.sendSimpleQuery(sql)
            ?: error("Query already released its Connection")
    }
}
