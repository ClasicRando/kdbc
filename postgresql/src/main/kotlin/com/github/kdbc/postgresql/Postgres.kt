package com.github.kdbc.postgresql

import com.github.kdbc.core.Database
import com.github.kdbc.postgresql.connection.PgBlockingConnection
import com.github.kdbc.postgresql.connection.PgConnectOptions
import com.github.kdbc.postgresql.connection.PgConnection
import com.github.kdbc.postgresql.pool.PgBlockingPoolManager
import com.github.kdbc.postgresql.pool.PgPoolManager

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
