package io.github.clasicrando.kdbc.postgresql

import io.github.clasicrando.kdbc.core.SslMode
import io.github.clasicrando.kdbc.core.stream.SocketOptions
import io.github.clasicrando.kdbc.postgresql.connection.PgConnectOptions
import io.github.clasicrando.kdbc.postgresql.connection.PgConnection
import io.github.clasicrando.kdbc.postgresql.listen.PgListener
import io.github.oshai.kotlinlogging.Level
import kotlin.time.DurationUnit
import kotlin.time.toDuration

object PgConnectionHelper {
    private val password = System.getenv("PG_TEST_PASSWORD")
    private val port = System.getenv("PG_TEST_PORT").toInt()

    val defaultConnectOptions =
        PgConnectOptions(
            host = "localhost",
            port = port,
            username = "postgres",
            password = password,
            applicationName = "KdbcTests",
            sslMode = SslMode.Disable,
            statementLogLevel = Level.INFO,
            socketOptions = SocketOptions(socketTimeout = 10.toDuration(DurationUnit.SECONDS)),
        )

    suspend fun defaultConnection(): PgConnection =
        Postgres.connection(connectOptions = defaultConnectOptions)

    suspend fun defaultListener(): PgListener =
        Postgres.listener(connectOptions = defaultConnectOptions)

    private val defaultConnectOptionsWithForcedSimple =
        defaultConnectOptions.copy(useExtendedProtocolForSimpleQueries = false)

    suspend fun defaultConnectionWithForcedSimple(): PgConnection =
        Postgres.connection(connectOptions = defaultConnectOptionsWithForcedSimple)

    private val defaultConnectOptionsWithQueryTimeout =
        defaultConnectOptionsWithForcedSimple.copy(
            queryTimeout = 2.toDuration(DurationUnit.SECONDS)
        )

    suspend fun defaultConnectionWithQueryTimeout(): PgConnection =
        Postgres.connection(connectOptions = defaultConnectOptionsWithQueryTimeout)

    private val defaultConnectOptionsSsl = defaultConnectOptions.copy(sslMode = SslMode.Require)

    suspend fun defaultConnectionSsl(): PgConnection =
        Postgres.connection(connectOptions = defaultConnectOptionsSsl)
}
