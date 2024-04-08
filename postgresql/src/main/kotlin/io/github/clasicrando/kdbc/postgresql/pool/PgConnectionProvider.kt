package io.github.clasicrando.kdbc.postgresql.pool

import io.github.clasicrando.kdbc.core.pool.ConnectionPool
import io.github.clasicrando.kdbc.core.pool.ConnectionProvider
import io.github.clasicrando.kdbc.core.stream.KtorAsyncStream
import io.github.clasicrando.kdbc.postgresql.connection.PgConnectOptions
import io.github.clasicrando.kdbc.postgresql.connection.PgConnection
import io.github.clasicrando.kdbc.postgresql.stream.PgStream
import io.ktor.network.sockets.InetSocketAddress

/**
 * Postgresql specific implementation for [ConnectionProvider] that provides the means to create
 * new [ConnectionPool] instances holding [PgConnection]s as well as validating that a
 * [PgConnection] is valid for reuse.
 */
internal class PgConnectionProvider(
    private val connectOptions: PgConnectOptions,
) : ConnectionProvider<PgConnection> {
    override suspend fun create(pool: ConnectionPool<PgConnection>): PgConnection {
        pool as PgConnectionPool
        val address = InetSocketAddress(connectOptions.host, connectOptions.port.toInt())
        val asyncStream = KtorAsyncStream(address, pool.selectorManager)
        var stream: PgStream? = null
        try {
            stream = PgStream.connect(
                scope = pool,
                asyncStream = asyncStream,
                connectOptions = connectOptions,
            )
            return PgConnection.connect(
                connectOptions = connectOptions,
                stream = stream,
                pool = pool,
            )
        } catch (ex: Throwable) {
            stream?.close()
            throw ex
        }
    }

    override suspend fun validate(connection: PgConnection): Boolean {
        if (connection.isConnected && connection.inTransaction) {
            connection.rollback()
        }
        return connection.isConnected && !connection.inTransaction && !connection.isWaiting
    }
}
