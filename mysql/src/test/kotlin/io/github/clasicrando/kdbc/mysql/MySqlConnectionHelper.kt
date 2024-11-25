package io.github.clasicrando.kdbc.mysql

import io.github.clasicrando.kdbc.core.SslMode
import io.github.clasicrando.kdbc.mysql.connection.Charset
import io.github.clasicrando.kdbc.mysql.connection.MySqlConnection
import io.github.clasicrando.kdbc.mysql.connection.MySqlConnectionOptions
import kotlin.time.DurationUnit
import kotlin.time.toDuration

object MySqlConnectionHelper {
    private val username = System.getenv("MYSQL_TEST_USER")
    private val password = System.getenv("MYSQL_TEST_PASSWORD")
    private val port = System.getenv("MYSQL_TEST_PORT").toInt()

    private val defaultConnectOptions =
        MySqlConnectionOptions(
            host = "localhost",
            port = port,
            username = username,
            password = password,
            database = "test",
            charset = Charset.Utf8mb4,
            sslMode = SslMode.Disable,
        )

    suspend fun defaultConnection(): MySqlConnection {
        return MySql.connection(connectOptions = defaultConnectOptions)
    }

    private val defaultTimeoutConnectOptions =
        defaultConnectOptions.copy(queryTimeout = 1.toDuration(DurationUnit.SECONDS))

    suspend fun defaultConnectionWithTimeout(): MySqlConnection {
        return MySql.connection(connectOptions = defaultTimeoutConnectOptions)
    }
}
