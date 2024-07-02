package io.github.clasicrando.kdbc.postgresql.pool

import io.github.clasicrando.kdbc.core.pool.AsyncConnectionPool
import io.github.clasicrando.kdbc.core.pool.AsyncConnectionProvider
import io.github.clasicrando.kdbc.core.stream.KtorAsyncStream
import io.github.clasicrando.kdbc.postgresql.connection.PgAsyncConnection
import io.github.clasicrando.kdbc.postgresql.connection.PgConnectOptions
import io.github.clasicrando.kdbc.postgresql.stream.PgAsyncStream
import io.ktor.network.sockets.InetSocketAddress

/**
 * Postgresql specific implementation for [AsyncConnectionProvider] that provides the means to
 * create new [AsyncConnectionPool] instances holding [PgAsyncConnection]s as well as
 * validating that a [PgAsyncConnection] is valid for reuse.
 */
internal class PgAsyncConnectionProvider(
    private val connectOptions: PgConnectOptions,
) : AsyncConnectionProvider<PgAsyncConnection> {
    override suspend fun create(pool: AsyncConnectionPool<PgAsyncConnection>): PgAsyncConnection {
        pool as PgAsyncConnectionPool
        val address = InetSocketAddress(connectOptions.host, connectOptions.port)
        val asyncStream = KtorAsyncStream(address, pool.selectorManager)
        var stream: PgAsyncStream? = null
        try {
            stream = PgAsyncStream.connect(
                scope = pool,
                asyncStream = asyncStream,
                connectOptions = connectOptions,
            )
            return PgAsyncConnection.connect(
                connectOptions = connectOptions,
                stream = stream,
                pool = pool,
            )
        } catch (ex: Throwable) {
            stream?.close()
            throw ex
        }
    }

    override suspend fun validate(connection: PgAsyncConnection): Boolean {
        if (connection.isConnected && connection.inTransaction) {
            connection.rollback()
        }
        return connection.isConnected && !connection.inTransaction
    }
}
