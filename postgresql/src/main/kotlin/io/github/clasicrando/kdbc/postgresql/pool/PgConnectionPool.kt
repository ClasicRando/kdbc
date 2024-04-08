package io.github.clasicrando.kdbc.postgresql.pool

import io.github.clasicrando.kdbc.core.pool.AbstractDefaultConnectionPool
import io.github.clasicrando.kdbc.core.pool.ConnectionPool
import io.github.clasicrando.kdbc.postgresql.column.PgTypeRegistry
import io.github.clasicrando.kdbc.postgresql.connection.PgConnectOptions
import io.github.clasicrando.kdbc.postgresql.connection.PgConnection
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
