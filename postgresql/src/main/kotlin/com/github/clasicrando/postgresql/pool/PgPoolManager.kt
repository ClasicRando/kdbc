package com.github.clasicrando.postgresql.pool

import com.github.clasicrando.common.pool.BasePoolManager
import com.github.clasicrando.common.pool.ConnectionPool
import com.github.clasicrando.postgresql.connection.PgConnectOptions
import com.github.clasicrando.postgresql.connection.PgConnection

internal object PgPoolManager : BasePoolManager<PgConnectOptions, PgConnection>() {
    override fun createPool(options: PgConnectOptions): ConnectionPool<PgConnection> {
        return PgConnectionPool(options)
    }
}