package io.github.clasicrando.kdbc.postgresql

import io.github.clasicrando.kdbc.postgresql.connection.PgBlockingConnection
import io.github.clasicrando.kdbc.postgresql.connection.PgConnectOptions
import io.github.clasicrando.kdbc.postgresql.connection.PgSuspendingConnection
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

    suspend fun defaultSuspendingConnection(): PgSuspendingConnection {
        return Postgres.suspendingConnection(connectOptions = defaultConnectOptions)
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

    suspend fun defaultSuspendingConnectionWithForcedSimple(): PgSuspendingConnection {
        return Postgres.suspendingConnection(connectOptions = defaultConnectOptionsWithForcedSimple)
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

    suspend fun defaultSuspendingConnectionWithQueryTimeout(): PgSuspendingConnection {
        return Postgres.suspendingConnection(connectOptions = defaultConnectOptionsWithQueryTimeout)
    }

    fun defaultBlockingConnectionWithQueryTimeout(): PgBlockingConnection {
        return Postgres.blockingConnection(connectOptions = defaultConnectOptionsWithQueryTimeout)
    }
}
