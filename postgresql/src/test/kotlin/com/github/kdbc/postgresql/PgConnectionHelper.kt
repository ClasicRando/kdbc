package com.github.kdbc.postgresql

import com.github.kdbc.postgresql.connection.PgBlockingConnection
import com.github.kdbc.postgresql.connection.PgConnectOptions
import com.github.kdbc.postgresql.connection.PgConnection

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

    fun defaultBlockingConnection(): PgBlockingConnection {
        return Postgres.blockingConnection(connectOptions = defaultConnectOptions)
    }

    fun defaultBlockingConnectionWithForcedSimple(): PgBlockingConnection {
        return Postgres.blockingConnection(connectOptions = defaultConnectOptionsWithForcedSimple)
    }
}
