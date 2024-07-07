package io.github.clasicrando.kdbc.postgresql

import io.github.clasicrando.kdbc.core.pool.PoolOptions
import io.github.clasicrando.kdbc.postgresql.connection.PgBlockingConnection
import io.github.clasicrando.kdbc.postgresql.connection.PgConnectOptions
import io.github.clasicrando.kdbc.postgresql.connection.PgAsyncConnection
import io.github.clasicrando.kdbc.postgresql.pool.PgAsyncConnectionPool
import io.github.clasicrando.kdbc.postgresql.pool.PgBlockingConnectionPool
import kotlin.time.DurationUnit
import kotlin.time.toDuration

object PgConnectionHelper {
    private val defaultConnectOptions = PgConnectOptions(
        host = "localhost",
        port = 5432,
        username = "postgres",
        password = System.getenv("PG_TEST_PASSWORD"),
        applicationName = "KdbcTests",
    )

    fun defaultAsyncPool(): PgAsyncConnectionPool {
        return PgAsyncConnectionPool(defaultConnectOptions, PoolOptions())
    }

    fun defaultBlockingPool(): PgBlockingConnectionPool {
        return PgBlockingConnectionPool(defaultConnectOptions, PoolOptions())
    }

    suspend fun defaultAsyncConnection(): PgAsyncConnection {
        return Postgres.asyncConnection(connectOptions = defaultConnectOptions)
    }

    fun defaultBlockingConnection(): PgBlockingConnection {
        return Postgres.blockingConnection(connectOptions = defaultConnectOptions)
    }

    private val defaultConnectOptionsWithForcedSimple = PgConnectOptions(
        host = "localhost",
        port = 5432,
        username = "postgres",
        password = System.getenv("PG_TEST_PASSWORD"),
        applicationName = "KdbcTests",
        useExtendedProtocolForSimpleQueries = false,
    )

    suspend fun defaultAsyncConnectionWithForcedSimple(): PgAsyncConnection {
        return Postgres.asyncConnection(connectOptions = defaultConnectOptionsWithForcedSimple)
    }

    fun defaultBlockingConnectionWithForcedSimple(): PgBlockingConnection {
        return Postgres.blockingConnection(connectOptions = defaultConnectOptionsWithForcedSimple)
    }

    private val defaultConnectOptionsWithQueryTimeout = PgConnectOptions(
        host = "localhost",
        port = 5432,
        username = "postgres",
        password = System.getenv("PG_TEST_PASSWORD"),
        applicationName = "KdbcTests",
        useExtendedProtocolForSimpleQueries = false,
        queryTimeout = 2.toDuration(DurationUnit.SECONDS)
    )

    suspend fun defaultAsyncConnectionWithQueryTimeout(): PgAsyncConnection {
        return Postgres.asyncConnection(connectOptions = defaultConnectOptionsWithQueryTimeout)
    }

    fun defaultBlockingConnectionWithQueryTimeout(): PgBlockingConnection {
        return Postgres.blockingConnection(connectOptions = defaultConnectOptionsWithQueryTimeout)
    }
}
