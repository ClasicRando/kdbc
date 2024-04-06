package com.github.clasicrando.postgresql.pool

import com.github.clasicrando.common.pool.AbstractDefaultBlockingConnectionPool
import com.github.clasicrando.common.pool.BlockingConnectionPool
import com.github.clasicrando.postgresql.column.PgTypeRegistry
import com.github.clasicrando.postgresql.connection.PgBlockingConnection
import com.github.clasicrando.postgresql.connection.PgConnectOptions

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
