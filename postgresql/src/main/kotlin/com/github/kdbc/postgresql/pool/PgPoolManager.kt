package com.github.kdbc.postgresql.pool

import com.github.kdbc.core.pool.BasePoolManager
import com.github.kdbc.core.pool.ConnectionPool
import com.github.kdbc.postgresql.connection.PgConnectOptions
import com.github.kdbc.postgresql.connection.PgConnection

/**
 * Postgresql specific implementation for a [BasePoolManager] that keeps track of
 * [PgConnectionPool] instances per unique [PgConnectOptions].
 */
internal object PgPoolManager : BasePoolManager<PgConnectOptions, PgConnection>() {
    override fun createPool(options: PgConnectOptions): ConnectionPool<PgConnection> {
        return PgConnectionPool(options)
    }
}