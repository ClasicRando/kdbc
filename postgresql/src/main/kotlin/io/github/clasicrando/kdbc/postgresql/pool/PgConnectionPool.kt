package io.github.clasicrando.kdbc.postgresql.pool

import io.github.clasicrando.kdbc.core.pool.AbstractDefaultConnectionPool
import io.github.clasicrando.kdbc.core.pool.PoolOptions
import io.github.clasicrando.kdbc.postgresql.connection.PgConnectOptions
import io.github.clasicrando.kdbc.postgresql.connection.PgConnection
import io.github.clasicrando.kdbc.postgresql.type.PgTypeCache
import io.ktor.network.selector.SelectorManager
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext

/**
 * Postgresql specific implementation of a
 * [io.github.clasicrando.kdbc.core.pool.ConnectionPool], keeping reference to the pool's
 * [typeCache] and providing the custom [disposeConnection] method that simple calls
 * [PgConnection.dispose].
 */
class PgConnectionPool(
    connectOptions: PgConnectOptions,
    poolOptions: PoolOptions,
) : AbstractDefaultConnectionPool<PgConnection>(
        poolOptions = poolOptions,
        provider = PgConnectionProvider(connectOptions),
    ) {
    internal val typeCache = PgTypeCache()
    internal val selectorManager = SelectorManager(dispatcher = this.coroutineContext)

    override suspend fun disposeConnection(connection: PgConnection) {
        connection.dispose()
    }

    override suspend fun close() {
        withContext(Dispatchers.IO) { selectorManager.close() }
        super.close()
    }
}
