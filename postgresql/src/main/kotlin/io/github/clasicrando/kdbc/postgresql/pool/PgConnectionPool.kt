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
 * Postgresql specific implementation of a [io.github.clasicrando.kdbc.core.pool.ConnectionPool],
 * keeping reference to the pool's [typeCache] and providing the custom [disposeConnection] method
 * that simple calls [PgConnection.dispose].
 */
public class PgConnectionPool(
    private val connectOptions: PgConnectOptions,
    poolOptions: PoolOptions,
) : AbstractDefaultConnectionPool<PgConnection>(poolOptions = poolOptions) {
    internal val typeCache = PgTypeCache(connectOptions.timeZoneOffset)
    internal val selectorManager = SelectorManager(dispatcher = this.coroutineContext)

    init {
        initializePool()
    }

    override suspend fun create(): PgConnection {
        return PgConnection.connect(connectOptions, this)
    }

    override suspend fun disposeConnection(connection: PgConnection) {
        connection.dispose()
    }

    override suspend fun close() {
        super.close()
        withContext(Dispatchers.IO) { selectorManager.close() }
    }
}
