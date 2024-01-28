package com.github.clasicrando.postgresql.pool

import com.github.clasicrando.common.pool.AbstractPoolManager
import com.github.clasicrando.common.pool.ConnectionPool
import com.github.clasicrando.postgresql.connection.PgConnectOptions
import com.github.clasicrando.postgresql.connection.PgConnection

object PgPoolManager : AbstractPoolManager<PgConnectOptions, PgConnection>() {
    override fun createPool(options: PgConnectOptions): ConnectionPool<PgConnection> {
        return PgConnectionPool(options)
    }
}