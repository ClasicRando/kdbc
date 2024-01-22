package com.github.clasicrando.postgresql.pool

import com.github.clasicrando.common.pool.AbstractConnectionPool
import com.github.clasicrando.postgresql.connection.PgConnectOptions
import com.github.clasicrando.postgresql.connection.PgConnection

internal class PgConnectionPool(
    connectOptions: PgConnectOptions
) : AbstractConnectionPool<PgConnection>(
    poolOptions = connectOptions.poolOptions,
    provider = PgConnectionProvider(connectOptions),
) {
    override suspend fun disposeConnection(connection: PgConnection) {
        connection.dispose()
    }
}
