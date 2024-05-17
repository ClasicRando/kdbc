package io.github.clasicrando.kdbc.postgresql

import io.github.clasicrando.kdbc.core.Database
import io.github.clasicrando.kdbc.postgresql.connection.PgBlockingConnection
import io.github.clasicrando.kdbc.postgresql.connection.PgConnectOptions
import io.github.clasicrando.kdbc.postgresql.connection.PgSuspendingConnection
import io.github.clasicrando.kdbc.postgresql.pool.PgBlockingPoolManager
import io.github.clasicrando.kdbc.postgresql.pool.PgSuspendingPoolManager

/** [Database] implementation for Postgresql */
object Postgres : Database<PgBlockingConnection, PgSuspendingConnection, PgConnectOptions> {
    /**
     * Create a new [PgSuspendingConnection] (or reuse an existing connection if any are available)
     * using the supplied [PgConnectOptions].
     */
    override suspend fun suspendingConnection(connectOptions: PgConnectOptions): PgSuspendingConnection {
        return PgSuspendingPoolManager.acquireConnection(connectOptions)
    }

    /**
     * Create a new [PgBlockingConnection] (or reuse an existing connection if any are available)
     * using the supplied [PgConnectOptions].
     */
    override fun blockingConnection(connectOptions: PgConnectOptions): PgBlockingConnection {
        return PgBlockingPoolManager.acquireConnection(connectOptions)
    }
}
