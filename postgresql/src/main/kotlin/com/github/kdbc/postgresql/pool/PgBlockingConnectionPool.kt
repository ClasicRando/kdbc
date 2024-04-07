package com.github.kdbc.postgresql.pool

import com.github.kdbc.core.pool.AbstractDefaultBlockingConnectionPool
import com.github.kdbc.core.pool.BlockingConnectionPool
import com.github.kdbc.postgresql.column.PgTypeRegistry
import com.github.kdbc.postgresql.connection.PgBlockingConnection
import com.github.kdbc.postgresql.connection.PgConnectOptions

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
