package io.github.clasicrando.kdbc.postgresql.pool

import io.github.clasicrando.kdbc.core.pool.BaseBlockingPoolManager
import io.github.clasicrando.kdbc.core.pool.BlockingConnectionPool
import io.github.clasicrando.kdbc.postgresql.connection.PgBlockingConnection
import io.github.clasicrando.kdbc.postgresql.connection.PgConnectOptions

/**
 * Postgresql specific implementation for a [BaseBlockingPoolManager] that keeps track of
 * [PgBlockingConnectionPool] instances per unique [PgConnectOptions].
 */
internal object PgBlockingPoolManager
    : BaseBlockingPoolManager<PgConnectOptions, PgBlockingConnection>()
{
    override fun createPool(
        options: PgConnectOptions,
    ): BlockingConnectionPool<PgBlockingConnection> {
        return PgBlockingConnectionPool(options)
    }
}