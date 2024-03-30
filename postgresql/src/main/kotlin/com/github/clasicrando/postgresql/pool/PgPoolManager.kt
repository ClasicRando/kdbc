package com.github.clasicrando.postgresql.pool

import com.github.clasicrando.common.pool.BasePoolManager
import com.github.clasicrando.common.pool.ConnectionPool
import com.github.clasicrando.postgresql.connection.PgConnectOptions
import com.github.clasicrando.postgresql.connection.PgConnection

/**
 * Postgresql specific implementation for a [BasePoolManager] that keeps track of
 * [PgConnectionPool] instances per unique [PgConnectOptions].
 */
internal object PgPoolManager : BasePoolManager<PgConnectOptions, PgConnection>() {
    override fun createPool(options: PgConnectOptions): ConnectionPool<PgConnection> {
        return PgConnectionPool(options)
    }
}