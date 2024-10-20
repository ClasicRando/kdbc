package io.github.clasicrando.kdbc.postgresql

import io.github.clasicrando.kdbc.core.SslMode
import io.github.clasicrando.kdbc.core.pool.PoolOptions
import io.github.clasicrando.kdbc.postgresql.connection.PgBlockingConnection
import io.github.clasicrando.kdbc.postgresql.connection.PgConnectOptions
import io.github.clasicrando.kdbc.postgresql.connection.PgAsyncConnection
import io.github.clasicrando.kdbc.postgresql.listen.PgAsyncListener
import io.github.clasicrando.kdbc.postgresql.listen.PgBlockingListener
import io.github.clasicrando.kdbc.postgresql.pool.PgAsyncConnectionPool
import io.github.clasicrando.kdbc.postgresql.pool.PgBlockingConnectionPool
import kotlin.time.DurationUnit
import kotlin.time.toDuration

object PgConnectionHelper {
    private val password = System.getenv("PG_TEST_PASSWORD")
    private val port = System.getenv("PG_TEST_PORT").toInt()

    private val defaultConnectOptions = PgConnectOptions(
        host = "localhost",
        port = port,
        username = "postgres",
        password = password,
        applicationName = "KdbcTests",
        sslMode = SslMode.Disable,
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

    suspend fun defaultAsyncListener(): PgAsyncListener {
        return Postgres.asyncListener(connectOptions = defaultConnectOptions)
    }

    fun defaultBlockingListener(): PgBlockingListener {
        return Postgres.blockingListener(connectOptions = defaultConnectOptions)
    }

    private val defaultConnectOptionsWithForcedSimple = PgConnectOptions(
        host = "localhost",
        port = port,
        username = "postgres",
        password = password,
        applicationName = "KdbcTests",
        sslMode = SslMode.Disable,
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
        port = port,
        username = "postgres",
        password = password,
        applicationName = "KdbcTests",
        sslMode = SslMode.Disable,
        useExtendedProtocolForSimpleQueries = false,
        queryTimeout = 2.toDuration(DurationUnit.SECONDS)
    )

    suspend fun defaultAsyncConnectionWithQueryTimeout(): PgAsyncConnection {
        return Postgres.asyncConnection(connectOptions = defaultConnectOptionsWithQueryTimeout)
    }

    fun defaultBlockingConnectionWithQueryTimeout(): PgBlockingConnection {
        return Postgres.blockingConnection(connectOptions = defaultConnectOptionsWithQueryTimeout)
    }

    private val defaultConnectOptionsSsl = PgConnectOptions(
        host = "localhost",
        port = port,
        username = "postgres",
        password = password,
        applicationName = "KdbcTests",
        sslMode = SslMode.Require,
        connectionTimeout = 1.toDuration(unit = DurationUnit.SECONDS),
    )

    suspend fun defaultAsyncConnectionSsl(): PgAsyncConnection {
        return Postgres.asyncConnection(connectOptions = defaultConnectOptionsSsl)
    }
}
