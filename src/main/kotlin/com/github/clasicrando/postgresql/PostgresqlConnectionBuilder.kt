package com.github.clasicrando.postgresql

import com.github.clasicrando.common.pool.ConnectionPool
import com.github.clasicrando.common.pool.PoolOptions
import com.github.clasicrando.postgresql.pool.PgConnectionProvider
import com.github.clasicrando.postgresql.pool.PgConnectionPoolImpl
import kotlinx.coroutines.CoroutineScope

object PostgresqlConnectionBuilder {
    suspend fun createConnection(
        scope: CoroutineScope,
        connectOptions: PgConnectOptions,
    ): PgConnection {
        val factory = PgConnectionProvider(connectOptions)
        return factory.create(scope) as PgConnection
    }

    fun createConnectionPool(
        poolOptions: PoolOptions,
        connectOptions: PgConnectOptions,
    ): ConnectionPool {
        val factory = PgConnectionProvider(connectOptions)
        return PgConnectionPoolImpl(poolOptions, factory)
    }
}