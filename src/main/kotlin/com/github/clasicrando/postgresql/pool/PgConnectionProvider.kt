package com.github.clasicrando.postgresql.pool

import com.github.clasicrando.common.pool.ConnectionProvider
import com.github.clasicrando.postgresql.connection.PgConnectOptions
import com.github.clasicrando.postgresql.connection.PgConnection
import com.github.clasicrando.postgresql.stream.PgStream
import kotlinx.coroutines.CoroutineScope

internal class PgConnectionProvider(
    private val connectOptions: PgConnectOptions,
) : ConnectionProvider<PgConnection> {
    override suspend fun create(scope: CoroutineScope): PgConnection {
        var stream: PgStream? = null
        try {
            stream = PgStream.connect(
                coroutineScope = scope,
                connectOptions = connectOptions,
            )
            return PgConnection.connect(
                connectOptions = connectOptions,
                stream = stream,
                scope = scope,
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
        return connection.isConnected && !connection.inTransaction
    }
}
