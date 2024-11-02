package io.github.clasicrando.kdbc.mysql.pool

import io.github.clasicrando.kdbc.core.pool.AbstractDefaultConnectionPool
import io.github.clasicrando.kdbc.core.pool.PoolOptions
import io.github.clasicrando.kdbc.mysql.connection.MySqlConnection
import io.github.clasicrando.kdbc.mysql.connection.MySqlConnectionOptions
import io.github.clasicrando.kdbc.mysql.type.MySqlTypeCache
import io.ktor.network.selector.SelectorManager
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext

/**
 * Postgresql specific implementation of a
 * [io.github.clasicrando.kdbc.core.pool.ConnectionPool], keeping reference to the pool's
 * [typeCache] and providing the custom [disposeConnection] method that simple calls
 * [MySqlConnection.dispose].
 */
class MySqlConnectionPool(
    connectOptions: MySqlConnectionOptions,
    poolOptions: PoolOptions,
) : AbstractDefaultConnectionPool<MySqlConnection>(
        poolOptions = poolOptions,
        provider = MySqlConnectionProvider(connectOptions),
    ) {
    internal val typeCache = MySqlTypeCache()
    internal val selectorManager = SelectorManager(dispatcher = this.coroutineContext)

    override suspend fun disposeConnection(connection: MySqlConnection) {
        connection.dispose()
    }

    override suspend fun close() {
        withContext(Dispatchers.IO) { selectorManager.close() }
        super.close()
    }
}
