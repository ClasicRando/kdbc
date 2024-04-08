package io.github.clasicrando.kdbc.postgresql

import io.github.clasicrando.kdbc.core.Database
import io.github.clasicrando.kdbc.postgresql.connection.PgBlockingConnection
import io.github.clasicrando.kdbc.postgresql.connection.PgConnectOptions
import io.github.clasicrando.kdbc.postgresql.connection.PgConnection
import io.github.clasicrando.kdbc.postgresql.pool.PgBlockingPoolManager
import io.github.clasicrando.kdbc.postgresql.pool.PgPoolManager

/** [Database] implementation for Postgresql */
object Postgres : Database<PgBlockingConnection, PgConnection, PgConnectOptions> {
    /**
     * Create a new [PgConnection] (or reuse an existing connection if any are available) using
     * the supplied [PgConnectOptions].
     */
    override suspend fun connection(connectOptions: PgConnectOptions): PgConnection {
        return PgPoolManager.acquireConnection(connectOptions)
    }

    /**
     * Create a new [PgBlockingConnection] (or reuse an existing connection if any are
     * available) using the supplied [PgConnectOptions].
     */
    override fun blockingConnection(connectOptions: PgConnectOptions): PgBlockingConnection {
        return PgBlockingPoolManager.acquireConnection(connectOptions)
    }
}
