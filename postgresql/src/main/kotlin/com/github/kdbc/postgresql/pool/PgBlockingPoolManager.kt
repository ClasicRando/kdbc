package com.github.kdbc.postgresql.pool

import com.github.kdbc.core.pool.BaseBlockingPoolManager
import com.github.kdbc.core.pool.BlockingConnectionPool
import com.github.kdbc.postgresql.connection.PgBlockingConnection
import com.github.kdbc.postgresql.connection.PgConnectOptions

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