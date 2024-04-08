package io.github.clasicrando.kdbc.postgresql.pool

import io.github.clasicrando.kdbc.core.pool.BlockingConnectionPool
import io.github.clasicrando.kdbc.core.pool.BlockingConnectionProvider
import io.github.clasicrando.kdbc.core.stream.SocketBlockingStream
import io.github.clasicrando.kdbc.postgresql.connection.PgBlockingConnection
import io.github.clasicrando.kdbc.postgresql.connection.PgConnectOptions
import io.github.clasicrando.kdbc.postgresql.stream.PgBlockingStream
import io.ktor.network.sockets.InetSocketAddress

/**
 * Postgresql specific implementation for [BlockingConnectionProvider] that provides the means to
 * create new [BlockingConnectionPool] instances holding [PgBlockingConnection]s as well as
 * validating that as [PgBlockingConnection] is valid for reuse.
 */
internal class PgBlockingConnectionProvider(
    private val connectOptions: PgConnectOptions,
) : BlockingConnectionProvider<PgBlockingConnection> {
    override fun create(pool: BlockingConnectionPool<PgBlockingConnection>): PgBlockingConnection {
        val address = InetSocketAddress(connectOptions.host, connectOptions.port.toInt())
        val blockingStream = SocketBlockingStream(address)
        var stream: PgBlockingStream? = null
        try {
            stream = PgBlockingStream.connect(
                blockingStream = blockingStream,
                connectOptions = connectOptions,
            )
            return PgBlockingConnection.connect(
                connectOptions = connectOptions,
                stream = stream,
                pool = pool as PgBlockingConnectionPool,
            )
        } catch (ex: Throwable) {
            stream?.close()
            throw ex
        }
    }

    override fun validate(connection: PgBlockingConnection): Boolean {
        if (connection.isConnected && connection.inTransaction) {
            connection.rollback()
        }
        return connection.isConnected && !connection.inTransaction
    }
}
