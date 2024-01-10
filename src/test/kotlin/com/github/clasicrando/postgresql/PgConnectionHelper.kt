package com.github.clasicrando.postgresql

import com.github.clasicrando.common.pool.ConnectionPool
import com.github.clasicrando.common.pool.PoolOptions
import com.github.clasicrando.postgresql.connection.PgConnectOptions
import com.github.clasicrando.postgresql.connection.PgConnection
import com.github.clasicrando.postgresql.connection.PostgresqlConnectionBuilder
import kotlinx.coroutines.CoroutineScope

object PgConnectionHelper {
    private val defaultConnectOptions = PgConnectOptions(
        host = "localhost",
        port = 5432U,
        username = "postgres",
        password = System.getenv("PG_TEST_PASSWORD"),
        applicationName = "KdbcTests",
    )
    private val defaultPoolOptions = PoolOptions(maxConnections = 1)

    suspend fun defaultConnection(scope: CoroutineScope): PgConnection {
        return PostgresqlConnectionBuilder.createConnection(
            scope = scope,
            connectOptions = defaultConnectOptions,
        )
    }

    fun defaultConnectionPool(): ConnectionPool {
        return PostgresqlConnectionBuilder.createConnectionPool(
            poolOptions = defaultPoolOptions,
            connectOptions = defaultConnectOptions,
        )
    }
}
