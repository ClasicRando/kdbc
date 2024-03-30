package com.github.clasicrando.postgresql.pool

import com.github.clasicrando.common.pool.AbstractDefaultConnectionPool
import com.github.clasicrando.common.pool.ConnectionPool
import com.github.clasicrando.postgresql.column.PgTypeRegistry
import com.github.clasicrando.postgresql.connection.PgConnectOptions
import com.github.clasicrando.postgresql.connection.PgConnection

/**
 * Postgresql specific implementation of a [ConnectionPool], keeping reference to the pool's
 * [typeRegistry] and providing the custom [disposeConnection] method that simple calls
 * [PgConnection.dispose].
 */
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
