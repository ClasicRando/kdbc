package com.github.clasicrando.postgresql.pool

import com.github.clasicrando.common.pool.AbstractDefaultConnectionPool
import com.github.clasicrando.common.pool.ConnectionPool
import com.github.clasicrando.postgresql.column.PgTypeRegistry
import com.github.clasicrando.postgresql.connection.PgConnectOptions
import com.github.clasicrando.postgresql.connection.PgConnection
import io.ktor.network.selector.SelectorManager
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext

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
    val typeRegistry = PgTypeRegistry()
    val selectorManager = SelectorManager(dispatcher = this.coroutineContext)

    override suspend fun disposeConnection(connection: PgConnection) {
        connection.dispose()
    }

    override suspend fun close() {
        withContext(Dispatchers.IO) { selectorManager.close() }
        super.close()
    }
}
