package io.github.clasicrando.kdbc.postgresql.pool

import io.github.clasicrando.kdbc.core.pool.SuspendingConnectionPool
import io.github.clasicrando.kdbc.core.pool.SuspendingConnectionProvider
import io.github.clasicrando.kdbc.core.stream.KtorAsyncStream
import io.github.clasicrando.kdbc.postgresql.connection.PgConnectOptions
import io.github.clasicrando.kdbc.postgresql.connection.PgSuspendingConnection
import io.github.clasicrando.kdbc.postgresql.stream.PgStream
import io.ktor.network.sockets.InetSocketAddress

/**
 * Postgresql specific implementation for [SuspendingConnectionProvider] that provides the means to
 * create new [SuspendingConnectionPool] instances holding [PgSuspendingConnection]s as well as
 * validating that a [PgSuspendingConnection] is valid for reuse.
 */
internal class PgSuspendingConnectionProvider(
    private val connectOptions: PgConnectOptions,
) : SuspendingConnectionProvider<PgSuspendingConnection> {
    override suspend fun create(pool: SuspendingConnectionPool<PgSuspendingConnection>): PgSuspendingConnection {
        pool as PgSuspendingConnectionPool
        val address = InetSocketAddress(connectOptions.host, connectOptions.port)
        val asyncStream = KtorAsyncStream(address, pool.selectorManager)
        var stream: PgStream? = null
        try {
            stream = PgStream.connect(
                scope = pool,
                asyncStream = asyncStream,
                connectOptions = connectOptions,
            )
            return PgSuspendingConnection.connect(
                connectOptions = connectOptions,
                stream = stream,
                pool = pool,
            )
        } catch (ex: Throwable) {
            stream?.close()
            throw ex
        }
    }

    override suspend fun validate(connection: PgSuspendingConnection): Boolean {
        if (connection.isConnected && connection.inTransaction) {
            connection.rollback()
        }
        return connection.isConnected && !connection.inTransaction
    }
}
