package io.github.clasicrando.kdbc.postgresql.pool

import io.github.clasicrando.kdbc.core.pool.AbstractDefaultAsyncConnectionPool
import io.github.clasicrando.kdbc.core.pool.PoolOptions
import io.github.clasicrando.kdbc.postgresql.column.PgTypeCache
import io.github.clasicrando.kdbc.postgresql.connection.PgAsyncConnection
import io.github.clasicrando.kdbc.postgresql.connection.PgConnectOptions
import io.ktor.network.selector.SelectorManager
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext

/**
 * Postgresql specific implementation of a
 * [io.github.clasicrando.kdbc.core.pool.AsyncConnectionPool], keeping reference to the pool's
 * [typeCache] and providing the custom [disposeConnection] method that simple calls
 * [PgAsyncConnection.dispose].
 */
class PgAsyncConnectionPool(
    connectOptions: PgConnectOptions,
    poolOptions: PoolOptions,
) : AbstractDefaultAsyncConnectionPool<PgAsyncConnection>(
    poolOptions = poolOptions,
    provider = PgAsyncConnectionProvider(connectOptions),
) {
    internal val typeCache = PgTypeCache()
    internal val selectorManager = SelectorManager(dispatcher = this.coroutineContext)

    override suspend fun disposeConnection(connection: PgAsyncConnection) {
        connection.dispose()
    }

    override suspend fun close() {
        withContext(Dispatchers.IO) { selectorManager.close() }
        super.close()
    }
}
