package io.github.clasicrando.kdbc.postgresql.pool

import io.github.clasicrando.kdbc.core.pool.BaseSuspendingPoolManager
import io.github.clasicrando.kdbc.core.pool.SuspendingConnectionPool
import io.github.clasicrando.kdbc.postgresql.connection.PgConnectOptions
import io.github.clasicrando.kdbc.postgresql.connection.PgSuspendingConnection

/**
 * Postgresql specific implementation for a [BaseSuspendingPoolManager] that keeps track of
 * [PgSuspendingConnectionPool] instances per unique [PgConnectOptions].
 */
internal object PgSuspendingPoolManager : BaseSuspendingPoolManager<PgConnectOptions, PgSuspendingConnection>() {
    override fun createPool(options: PgConnectOptions): SuspendingConnectionPool<PgSuspendingConnection> {
        return PgSuspendingConnectionPool(options)
    }
}