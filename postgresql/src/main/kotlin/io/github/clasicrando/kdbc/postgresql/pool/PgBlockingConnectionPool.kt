package io.github.clasicrando.kdbc.postgresql.pool

import io.github.clasicrando.kdbc.core.pool.AbstractDefaultBlockingConnectionPool
import io.github.clasicrando.kdbc.core.pool.BlockingConnectionPool
import io.github.clasicrando.kdbc.postgresql.column.PgTypeRegistry
import io.github.clasicrando.kdbc.postgresql.connection.PgBlockingConnection
import io.github.clasicrando.kdbc.postgresql.connection.PgConnectOptions

/**
 * Postgresql specific implementation of a [BlockingConnectionPool], keeping reference to the
 * pool's [typeRegistry] and providing the custom [disposeConnection] method that simple calls
 * [PgBlockingConnection.dispose].
 */
internal class PgBlockingConnectionPool(
    connectOptions: PgConnectOptions
) : AbstractDefaultBlockingConnectionPool<PgBlockingConnection>(
    poolOptions = connectOptions.poolOptions,
    provider = PgBlockingConnectionProvider(connectOptions),
) {
    val typeRegistry = PgTypeRegistry()

    override fun disposeConnection(connection: PgBlockingConnection) {
        connection.dispose()
    }
}
