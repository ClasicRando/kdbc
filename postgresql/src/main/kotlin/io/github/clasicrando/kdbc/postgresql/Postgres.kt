package io.github.clasicrando.kdbc.postgresql

import io.github.clasicrando.kdbc.core.Database
import io.github.clasicrando.kdbc.postgresql.connection.PgAsyncConnection
import io.github.clasicrando.kdbc.postgresql.connection.PgBlockingConnection
import io.github.clasicrando.kdbc.postgresql.connection.PgConnectOptions
import io.github.clasicrando.kdbc.postgresql.listen.PgAsyncListener
import io.github.clasicrando.kdbc.postgresql.listen.PgBlockingListener
import io.github.clasicrando.kdbc.postgresql.pool.PgAsyncPoolManager
import io.github.clasicrando.kdbc.postgresql.pool.PgBlockingPoolManager

/** [Database] implementation for Postgresql */
object Postgres : Database<PgBlockingConnection, PgAsyncConnection, PgConnectOptions> {
    /**
     * Create a new [PgAsyncConnection] (or reuse an existing connection if any are available)
     * using the supplied [PgConnectOptions].
     */
    override suspend fun asyncConnection(connectOptions: PgConnectOptions): PgAsyncConnection {
        return PgAsyncPoolManager.acquireConnection(connectOptions)
    }

    /**
     * Create a new [PgBlockingConnection] (or reuse an existing connection if any are available)
     * using the supplied [PgConnectOptions].
     */
    override fun blockingConnection(connectOptions: PgConnectOptions): PgBlockingConnection {
        return PgBlockingPoolManager.acquireConnection(connectOptions)
    }

    /**
     * Create a new [PgAsyncListener] with a connection acquired from [asyncConnection]
     */
    suspend fun asyncListener(connectOptions: PgConnectOptions): PgAsyncListener {
        return PgAsyncListener(asyncConnection(connectOptions))
    }

    /**
     * Create a new [PgBlockingListener] with a connection acquired from [blockingConnection]
     */
    fun blockingListener(connectOptions: PgConnectOptions): PgBlockingListener {
        return PgBlockingListener(blockingConnection(connectOptions))
    }
}
