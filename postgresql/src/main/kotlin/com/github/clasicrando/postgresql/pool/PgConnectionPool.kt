package com.github.clasicrando.postgresql.pool

import com.github.clasicrando.common.pool.AbstractDefaultConnectionPool
import com.github.clasicrando.postgresql.column.PgTypeRegistry
import com.github.clasicrando.postgresql.connection.PgConnectOptions
import com.github.clasicrando.postgresql.connection.PgConnection

internal class PgConnectionPool(
    connectOptions: PgConnectOptions
) : AbstractDefaultConnectionPool<PgConnection>(
    poolOptions = connectOptions.poolOptions,
    provider = PgConnectionProvider(connectOptions),
) {
    internal val typeRegistry = PgTypeRegistry()

    override suspend fun disposeConnection(connection: PgConnection) {
        connection.dispose()
    }
}
