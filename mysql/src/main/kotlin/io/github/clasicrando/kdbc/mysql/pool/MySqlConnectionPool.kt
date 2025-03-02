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
 * MySQL specific implementation of a [io.github.clasicrando.kdbc.core.pool.ConnectionPool], keeping
 * reference to the pool's [typeCache] and providing the custom [disposeConnection] method that
 * simple calls [MySqlConnection.dispose].
 */
public class MySqlConnectionPool(
    private val connectOptions: MySqlConnectionOptions,
    poolOptions: PoolOptions,
) : AbstractDefaultConnectionPool<MySqlConnection>(poolOptions = poolOptions) {
    internal val typeCache = MySqlTypeCache(connectOptions.timeZoneOffset)
    internal val selectorManager = SelectorManager(dispatcher = this.coroutineContext)

    init {
        initializePool()
    }

    override suspend fun create(): MySqlConnection {
        return MySqlConnection.connect(connectOptions, pool = this)
    }

    override suspend fun disposeConnection(connection: MySqlConnection) {
        connection.dispose()
    }

    override suspend fun close() {
        withContext(Dispatchers.IO) { selectorManager.close() }
        super.close()
    }
}
