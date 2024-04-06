package com.github.clasicrando.postgresql

import com.github.clasicrando.postgresql.connection.PgBlockingConnection
import com.github.clasicrando.postgresql.connection.PgConnectOptions
import com.github.clasicrando.postgresql.connection.PgConnection

object PgConnectionHelper {
    private val defaultConnectOptions = PgConnectOptions(
        host = "localhost",
        port = 5432U,
        username = "postgres",
        password = System.getenv("PG_TEST_PASSWORD"),
        applicationName = "KdbcTests",
    )

    suspend fun defaultConnection(): PgConnection {
        return PgConnection.connect(connectOptions = defaultConnectOptions)
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
        return PgConnection.connect(connectOptions = defaultConnectOptionsWithForcedSimple)
    }

    fun defaultBlockingConnection(): PgBlockingConnection {
        return PgBlockingConnection.connect(connectOptions = defaultConnectOptions)
    }

    fun defaultBlockingConnectionWithForcedSimple(): PgBlockingConnection {
        return PgBlockingConnection.connect(connectOptions = defaultConnectOptionsWithForcedSimple)
    }
}
