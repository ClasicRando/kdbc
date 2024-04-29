package io.github.clasicrando.kdbc.postgresql.query

import io.github.clasicrando.kdbc.core.query.BaseQuery
import io.github.clasicrando.kdbc.core.query.Query
import io.github.clasicrando.kdbc.core.result.StatementResult
import io.github.clasicrando.kdbc.postgresql.connection.PgConnection
import io.github.clasicrando.kdbc.postgresql.connection.PgConnectOptions

/**
 * Postgresql implementation of a [Query] where the [sql] query is usually executed using the
 * simple query protocol unless the query text does not contain a semicolon and
 * [PgConnectOptions.useExtendedProtocolForSimpleQueries] is true (the default value).
 */
class PgQuery(connection: PgConnection, sql: String) : BaseQuery<PgConnection>(
    sql = sql,
    connection = connection,
) {
    override suspend fun vendorExecuteQuery(): StatementResult {
        return connection?.sendSimpleQuery(sql)
            ?: error("Query already released its Connection")
    }
}
