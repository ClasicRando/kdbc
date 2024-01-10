package com.github.clasicrando.postgresql.pool

import com.github.clasicrando.common.connection.Connection
import com.github.clasicrando.common.pool.ConnectionFactory
import com.github.clasicrando.postgresql.connection.PgConnectOptions
import com.github.clasicrando.postgresql.connection.PgConnection
import com.github.clasicrando.postgresql.connection.PgConnectionImpl
import com.github.clasicrando.postgresql.stream.PgStream
import kotlinx.coroutines.CoroutineScope

class PgConnectionFactory(private val connectOptions: PgConnectOptions) : ConnectionFactory {
    override suspend fun create(scope: CoroutineScope): Connection {
        var stream: PgStream? = null
        try {
            stream = PgStream.connect(
                coroutineScope = scope,
                connectOptions = connectOptions,
            )
            return PgConnectionImpl.connect(
                configuration = connectOptions,
                charset = Charsets.UTF_8,
                stream = stream,
                scope = scope,
            )
        } catch (ex: Throwable) {
            stream?.close()
            throw ex
        }
    }

    override suspend fun validate(item: Connection): Boolean {
        val connection = item as? PgConnection ?: return false
        if (connection.isConnected && connection.inTransaction) {
            connection.rollback()
        }
        return connection.isConnected && !connection.inTransaction
    }
}
