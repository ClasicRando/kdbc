package com.github.kdbc.postgresql

import com.github.kdbc.postgresql.connection.PgBlockingConnection
import com.github.kdbc.postgresql.connection.PgConnectOptions
import com.github.kdbc.postgresql.connection.PgConnection
import kotlin.time.DurationUnit
import kotlin.time.toDuration

object PgConnectionHelper {
    private val defaultConnectOptions = PgConnectOptions(
        host = "localhost",
        port = 5432U,
        username = "postgres",
        password = System.getenv("PG_TEST_PASSWORD"),
        applicationName = "KdbcTests",
    )

    suspend fun defaultConnection(): PgConnection {
        return Postgres.connection(connectOptions = defaultConnectOptions)
    }

    fun defaultBlockingConnection(): PgBlockingConnection {
        return Postgres.blockingConnection(connectOptions = defaultConnectOptions)
    }

    private val defaultConnectOptionsWithForcedSimple = PgConnectOptions(
        host = "localhost",
        port = 5432U,
        username = "postgres",
        password = System.getenv("PG_TEST_PASSWORD"),
        applicationName = "KdbcTests",
        useExtendedProtocolForSimpleQueries = false,
    )

    suspend fun defaultConnectionWithForcedSimple(): PgConnection {
        return Postgres.connection(connectOptions = defaultConnectOptionsWithForcedSimple)
    }

    fun defaultBlockingConnectionWithForcedSimple(): PgBlockingConnection {
        return Postgres.blockingConnection(connectOptions = defaultConnectOptionsWithForcedSimple)
    }

    private val defaultConnectOptionsWithQueryTimeout = PgConnectOptions(
        host = "localhost",
        port = 5432U,
        username = "postgres",
        password = System.getenv("PG_TEST_PASSWORD"),
        applicationName = "KdbcTests",
        useExtendedProtocolForSimpleQueries = false,
        queryTimeout = 2.toDuration(DurationUnit.SECONDS)
    )

    suspend fun defaultConnectionWithQueryTimeout(): PgConnection {
        return Postgres.connection(connectOptions = defaultConnectOptionsWithQueryTimeout)
    }

    fun defaultBlockingConnectionWithQueryTimeout(): PgBlockingConnection {
        return Postgres.blockingConnection(connectOptions = defaultConnectOptionsWithQueryTimeout)
    }
}
