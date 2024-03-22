package com.github.clasicrando.postgresql.pool

import com.github.clasicrando.common.pool.ConnectionPool
import com.github.clasicrando.common.pool.ConnectionProvider
import com.github.clasicrando.postgresql.connection.PgConnectOptions
import com.github.clasicrando.postgresql.connection.PgConnection
import com.github.clasicrando.postgresql.stream.PgStream

internal class PgConnectionProvider(
    private val connectOptions: PgConnectOptions,
) : ConnectionProvider<PgConnection> {
    override suspend fun create(pool: ConnectionPool<PgConnection>): PgConnection {
        var stream: PgStream? = null
        try {
            stream = PgStream.connect(
                scope = pool,
                connectOptions = connectOptions,
            )
            return PgConnection.connect(
                connectOptions = connectOptions,
                stream = stream,
                pool = pool as PgConnectionPool,
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
