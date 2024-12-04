package io.github.clasicrando.kdbc.mysql.pool

import io.github.clasicrando.kdbc.core.pool.ConnectionPool
import io.github.clasicrando.kdbc.core.pool.ConnectionProvider
import io.github.clasicrando.kdbc.core.stream.KtorStream
import io.github.clasicrando.kdbc.mysql.connection.MySqlConnection
import io.github.clasicrando.kdbc.mysql.connection.MySqlConnectionOptions
import io.github.clasicrando.kdbc.mysql.stream.MySqlStream
import io.ktor.network.sockets.InetSocketAddress

/**
 * MySQL specific implementation for [ConnectionProvider] that provides the means to create new
 * [ConnectionPool] instances holding [MySqlConnection]s as well as validating that a
 * [MySqlConnection] is valid for reuse.
 */
internal class MySqlConnectionProvider(private val connectOptions: MySqlConnectionOptions) :
    ConnectionProvider<MySqlConnection> {
    override suspend fun create(pool: ConnectionPool<MySqlConnection>): MySqlConnection {
        pool as MySqlConnectionPool
        val address = InetSocketAddress(connectOptions.host, connectOptions.port)
        val stream = KtorStream(address, pool.selectorManager)
        var mySqlStream: MySqlStream? = null
        try {
            mySqlStream = MySqlStream.connect(stream = stream, connectionOptions = connectOptions)
            return MySqlConnection.connect(
                connectionOptions = connectOptions,
                stream = mySqlStream,
                pool = pool,
            )
        } catch (ex: Exception) {
            mySqlStream?.close()
            throw ex
        }
    }

    override suspend fun validate(connection: MySqlConnection): Boolean {
        if (connection.isConnected && connection.inTransaction) {
            connection.rollback()
        }
        return connection.isConnected && !connection.inTransaction
    }
}
