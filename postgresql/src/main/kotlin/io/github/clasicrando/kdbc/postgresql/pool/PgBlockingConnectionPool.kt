package io.github.clasicrando.kdbc.postgresql.pool

import io.github.clasicrando.kdbc.core.pool.AbstractDefaultBlockingConnectionPool
import io.github.clasicrando.kdbc.core.pool.PoolOptions
import io.github.clasicrando.kdbc.postgresql.column.PgTypeCache
import io.github.clasicrando.kdbc.postgresql.connection.PgBlockingConnection
import io.github.clasicrando.kdbc.postgresql.connection.PgConnectOptions

/**
 * Postgresql specific implementation of a
 * [io.github.clasicrando.kdbc.core.pool.BlockingConnectionPool], keeping reference to the pool's
 * [typeCache] and providing the custom [disposeConnection] method that simple calls
 * [PgBlockingConnection.dispose].
 */
class PgBlockingConnectionPool(
    connectOptions: PgConnectOptions,
    poolOptions: PoolOptions,
) : AbstractDefaultBlockingConnectionPool<PgBlockingConnection>(
    poolOptions = poolOptions,
    provider = PgBlockingConnectionProvider(connectOptions),
) {
    internal val typeCache = PgTypeCache()

    override fun disposeConnection(connection: PgBlockingConnection) {
        connection.dispose()
    }
}
