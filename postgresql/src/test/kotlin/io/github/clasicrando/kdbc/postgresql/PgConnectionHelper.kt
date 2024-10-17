package io.github.clasicrando.kdbc.postgresql

import io.github.clasicrando.kdbc.core.SslMode
import io.github.clasicrando.kdbc.core.pool.PoolOptions
import io.github.clasicrando.kdbc.postgresql.connection.PgConnection
import io.github.clasicrando.kdbc.postgresql.connection.PgConnectOptions
import io.github.clasicrando.kdbc.postgresql.listen.PgListener
import io.github.clasicrando.kdbc.postgresql.pool.PgConnectionPool
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

    fun defaultPool(): PgConnectionPool {
        return PgConnectionPool(defaultConnectOptions, PoolOptions())
    }

    suspend fun defaultConnection(): PgConnection {
        return Postgres.connection(connectOptions = defaultConnectOptions)
    }

    suspend fun defaultListener(): PgListener {
        return Postgres.listener(connectOptions = defaultConnectOptions)
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

    suspend fun defaultConnectionWithForcedSimple(): PgConnection {
        return Postgres.connection(connectOptions = defaultConnectOptionsWithForcedSimple)
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

    suspend fun defaultConnectionWithQueryTimeout(): PgConnection {
        return Postgres.connection(connectOptions = defaultConnectOptionsWithQueryTimeout)
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

    suspend fun defaultConnectionSsl(): PgConnection {
        return Postgres.connection(connectOptions = defaultConnectOptionsSsl)
    }
}
