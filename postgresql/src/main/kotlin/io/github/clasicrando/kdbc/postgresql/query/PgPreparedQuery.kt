package io.github.clasicrando.kdbc.postgresql.query

import io.github.clasicrando.kdbc.core.query.BasePreparedQuery
import io.github.clasicrando.kdbc.core.result.StatementResult
import io.github.clasicrando.kdbc.postgresql.connection.PgConnection
import io.github.clasicrando.kdbc.core.query.PreparedQuery

/**
 * Postgresql implementation of a [PreparedQuery] where the [sql] query is executed using the
 * extended query protocol with the [parameters] provided.
 */
class PgPreparedQuery(
    connection: PgConnection,
    sql: String,
) : BasePreparedQuery<PgConnection>(connection = connection, sql = sql) {
    override suspend fun vendorExecuteQuery(): StatementResult {
        return connection!!.sendExtendedQuery(sql, innerParameters)
    }
}
