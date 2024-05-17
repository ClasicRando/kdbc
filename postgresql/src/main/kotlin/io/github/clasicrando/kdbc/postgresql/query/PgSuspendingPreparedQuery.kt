package io.github.clasicrando.kdbc.postgresql.query

import io.github.clasicrando.kdbc.core.query.BaseSuspendingPreparedQuery
import io.github.clasicrando.kdbc.core.query.SuspendingPreparedQuery
import io.github.clasicrando.kdbc.core.result.StatementResult
import io.github.clasicrando.kdbc.postgresql.connection.PgSuspendingConnection

/**
 * Postgresql implementation of a [SuspendingPreparedQuery] where the [sql] query is executed using
 * the extended query protocol with the [parameters] provided.
 */
class PgSuspendingPreparedQuery(
    connection: PgSuspendingConnection,
    sql: String,
) : BaseSuspendingPreparedQuery<PgSuspendingConnection>(connection = connection, sql = sql) {
    override suspend fun vendorExecuteQuery(): StatementResult {
        return connection!!.sendExtendedQuery(sql, innerParameters)
    }
}
