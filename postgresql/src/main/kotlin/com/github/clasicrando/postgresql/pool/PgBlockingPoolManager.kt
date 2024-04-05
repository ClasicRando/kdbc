package com.github.clasicrando.postgresql.pool

import com.github.clasicrando.common.pool.BaseBlockingPoolManager
import com.github.clasicrando.common.pool.BasePoolManager
import com.github.clasicrando.common.pool.BlockingConnectionPool
import com.github.clasicrando.postgresql.connection.PgBlockingConnection
import com.github.clasicrando.postgresql.connection.PgConnectOptions

/**
 * Postgresql specific implementation for a [BasePoolManager] that keeps track of
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