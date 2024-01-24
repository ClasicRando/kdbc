package com.github.clasicrando.postgresql.pool

import com.github.clasicrando.common.atomic.AtomicMutableMap
import com.github.clasicrando.common.exceptions.CouldNotInitializeConnection
import com.github.clasicrando.common.pool.ConnectionPool
import com.github.clasicrando.postgresql.connection.PgConnectOptions
import com.github.clasicrando.postgresql.connection.PgConnection

object PgPoolManager {
    private val connectionPools: MutableMap<PgConnectOptions, ConnectionPool<PgConnection>> = AtomicMutableMap()

    suspend fun createConnection(connectOptions: PgConnectOptions): PgConnection {
        return connectionPools.getOrPut(connectOptions) {
            val pool = PgConnectionPool(connectOptions)
            val isValid = pool.waitForValidation()
            if (!isValid) {
                throw CouldNotInitializeConnection(connectOptions.toString())
            }
            pool
        }.acquire()
    }
}