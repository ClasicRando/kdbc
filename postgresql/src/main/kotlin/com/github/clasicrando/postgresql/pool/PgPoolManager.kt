package com.github.clasicrando.postgresql.pool

import com.github.clasicrando.common.pool.AbstractPoolManager
import com.github.clasicrando.common.pool.ConnectionPool
import com.github.clasicrando.common.pool.KdbcPoolsManager
import com.github.clasicrando.postgresql.connection.PgConnectOptions
import com.github.clasicrando.postgresql.connection.PgConnection

internal object PgPoolManager : AbstractPoolManager<PgConnectOptions, PgConnection>() {
    init {
        KdbcPoolsManager.addPoolManager(this)
    }
    override fun createPool(options: PgConnectOptions): ConnectionPool<PgConnection> {
        return PgConnectionPool(options)
    }
}