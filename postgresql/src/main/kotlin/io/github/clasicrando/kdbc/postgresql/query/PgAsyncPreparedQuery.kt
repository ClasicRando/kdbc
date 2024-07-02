package io.github.clasicrando.kdbc.postgresql.query

import io.github.clasicrando.kdbc.core.query.BaseAsyncPreparedQuery
import io.github.clasicrando.kdbc.core.result.StatementResult
import io.github.clasicrando.kdbc.postgresql.connection.PgAsyncConnection

/**
 * Postgresql implementation of a [io.github.clasicrando.kdbc.core.query.AsyncPreparedQuery]
 * where the [sql] query is executed using the extended query protocol with the [parameters]
 * provided.
 */
internal class PgAsyncPreparedQuery(
    connection: PgAsyncConnection,
    sql: String,
) : BaseAsyncPreparedQuery<PgAsyncConnection>(connection = connection, sql = sql) {
    override suspend fun vendorExecuteQuery(): StatementResult {
        return connection!!.sendExtendedQuery(sql, innerParameters)
    }
}
