package io.github.clasicrando.kdbc.postgresql.query

import io.github.clasicrando.kdbc.core.query.BaseSuspendingQuery
import io.github.clasicrando.kdbc.core.result.StatementResult
import io.github.clasicrando.kdbc.postgresql.connection.PgSuspendingConnection

/**
 * Postgresql implementation of a [io.github.clasicrando.kdbc.core.query.SuspendingQuery] where the
 * [sql] query is usually executed using the simple query protocol unless the query text does not
 * contain a semicolon and
 * [io.github.clasicrando.kdbc.postgresql.connection.PgConnectOptions.useExtendedProtocolForSimpleQueries]
 * is true (the default value).
 */
class PgSuspendingQuery(
    connection: PgSuspendingConnection,
    sql: String,
) : BaseSuspendingQuery<PgSuspendingConnection>(sql = sql, connection = connection) {
    override suspend fun vendorExecuteQuery(): StatementResult {
        return connection?.sendSimpleQuery(sql)
            ?: error("Query already released its Connection")
    }
}
