package io.github.clasicrando.kdbc.postgresql.pool

import io.github.clasicrando.kdbc.core.pool.AbstractDefaultSuspendingConnectionPool
import io.github.clasicrando.kdbc.core.pool.SuspendingConnectionPool
import io.github.clasicrando.kdbc.postgresql.column.PgTypeRegistry
import io.github.clasicrando.kdbc.postgresql.connection.PgConnectOptions
import io.github.clasicrando.kdbc.postgresql.connection.PgSuspendingConnection
import io.ktor.network.selector.SelectorManager
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext

/**
 * Postgresql specific implementation of a [SuspendingConnectionPool], keeping reference to the
 * pool's [typeRegistry] and providing the custom [disposeConnection] method that simple calls
 * [PgSuspendingConnection.dispose].
 */
class PgSuspendingConnectionPool(
    connectOptions: PgConnectOptions
) : AbstractDefaultSuspendingConnectionPool<PgSuspendingConnection>(
    poolOptions = connectOptions.poolOptions,
    provider = PgSuspendingConnectionProvider(connectOptions),
) {
    val typeRegistry = PgTypeRegistry()
    val selectorManager = SelectorManager(dispatcher = this.coroutineContext)

    override suspend fun disposeConnection(connection: PgSuspendingConnection) {
        connection.dispose()
    }

    override suspend fun close() {
        withContext(Dispatchers.IO) { selectorManager.close() }
        super.close()
    }
}
