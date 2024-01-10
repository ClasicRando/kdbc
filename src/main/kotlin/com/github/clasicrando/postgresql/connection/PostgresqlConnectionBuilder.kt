package com.github.clasicrando.postgresql.connection

import com.github.clasicrando.common.pool.ConnectionPool
import com.github.clasicrando.common.pool.ConnectionPoolImpl
import com.github.clasicrando.common.pool.PoolOptions
import com.github.clasicrando.postgresql.pool.PgConnectionFactory
import kotlinx.coroutines.CoroutineScope

object PostgresqlConnectionBuilder {
    suspend fun createConnection(
        scope: CoroutineScope,
        connectOptions: PgConnectOptions,
    ): PgConnection {
        val factory = PgConnectionFactory(connectOptions)
        return factory.create(scope) as PgConnection
    }

    fun createConnectionPool(
        poolOptions: PoolOptions,
        connectOptions: PgConnectOptions,
    ): ConnectionPool {
        val factory = PgConnectionFactory(connectOptions)
        return ConnectionPoolImpl(poolOptions, factory)
    }
}