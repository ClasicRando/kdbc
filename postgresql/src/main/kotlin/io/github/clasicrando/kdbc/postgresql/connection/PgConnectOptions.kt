package io.github.clasicrando.kdbc.postgresql.connection

import io.github.clasicrando.kdbc.core.LogSettings
import io.github.clasicrando.kdbc.core.SslMode
import io.github.clasicrando.kdbc.core.isZeroOrInfinite
import io.github.oshai.kotlinlogging.Level
import io.ktor.network.tls.TLSConfigBuilder
import kotlinx.serialization.Serializable
import kotlinx.serialization.Transient
import kotlin.time.Duration
import kotlin.time.DurationUnit
import kotlin.time.toDuration

/** Connection options for a postgresql database */
@Serializable
data class PgConnectOptions(
    /** Host name or IP address of the postgresql server */
    val host: String,
    /** Port on the host machine of the postgresql server */
    val port: Int,
    /** Name of the user to log in to the postgresql server */
    val username: String,
    /** Optional application name to set as part of the connection context */
    val applicationName: String? = null,
    /** Timeout duration during initial TCP connection establishment */
    val connectionTimeout: Duration = 10.toDuration(DurationUnit.SECONDS),
    /** Password if the database instance requires a password */
    val password: String? = null,
    /**
     * Optional initial connection database name. If not specified then postgresql will assume a
     * database with the same name as the [username]
     */
    val database: String? = null,
    /** Statement logging settings. If not specified, [LogSettings.DEFAULT] is used. */
    val logSettings: LogSettings = LogSettings.DEFAULT,
    /**
     * The duration that is waited before canceling a query execution due to timeout. The default
     * is an infinite timeout which means it will never cancel a query.
     *
     * Note: [Duration.ZERO] is also treated as infinite and negative timeouts are ignored with the
     * default value is used
     */
    val queryTimeout: Duration = Duration.INFINITE,
    /** Size of the cache storing prepared statement on the client side */
    val statementCacheCapacity: Int = 100,
    /**
     * Flag allowing the connection to override a call to send a simple query to send a prepared
     * statement instead, so binary data transfer can be used. To make things simple, a connection
     * check that a query does not contain a ';' (since prepared statements only allow a single
     * statement per query) and if this flag is true. If either checks returns false, the simple
     * query protocol is used.
     */
    val useExtendedProtocolForSimpleQueries: Boolean = true,
    /**
     * This parameter adjusts the number of digits used for textual output of floating-point values,
     * including float4, float8, and geometric data types. Default is 1
     *
     * [docs](https://www.postgresql.org/docs/16/runtime-config-client.html#GUC-EXTRA-FLOAT-DIGITS)
     */
    val extraFloatDigits: Int = 1,
    /**
     * SSL Mode of the connection. Uses the default mode of [SslMode.Prefer]
     */
    val sslMode: SslMode = SslMode.DEFAULT,
    /**
     * The default schema within the database connection. Sets the `search_path` connection
     * parameter. When null specified (the default) then the default connection property is used
     * which is public.
     */
    val currentSchema: String? = null,
    /**
     * TLS Config builder action to modify the config provided to the ktor socket creator. This is
     * only used if the server supports TLS and the socket used to create the database connection
     * is a [io.github.clasicrando.kdbc.core.stream.KtorAsyncStream] (i.e. only for async
     * connections).
     */
    @Transient
    val tlsConfig: TLSConfigBuilder.() -> Unit = {},
) {
    /** Connection properties as they are sent to the database upon connection initialization */
    val properties: List<Pair<String, String>> = listOf(
        "user" to username,
        "database" to database,
        "client_encoding" to "UTF-8",
        "DateStyle" to "ISO",
        "intervalstyle" to "iso_8601",
        "TimeZone" to "UTC",
        "extra_float_digits" to extraFloatDigits.toString(),
        "search_path" to currentSchema,
        "bytea_output" to "hex",
        "application_name" to applicationName,
        "statement_timeout" to queryTimeout.coerceAtLeast(Duration.ZERO).let {
            if (it.isZeroOrInfinite()) {
                "0"
            } else {
                it.inWholeMilliseconds.coerceAtMost(Int.MAX_VALUE.toLong()).toString()
            }
        }
    ).mapNotNull { (key, value) ->
        value?.let { key to it }
    }

    /**
     * Return a shallow copy of the current [PgConnectOptions] with the log statement [level]
     * altered
     */
    fun logStatements(level: Level): PgConnectOptions {
        val newLogSettings = logSettings.copy(statementLevel = level)
        return copy(logSettings = newLogSettings)
    }

    /**
     * Return a shallow copy of the current [PgConnectOptions] with the new log slow statement
     * [level] and [duration] altered
     */
    fun logSlowStatements(level: Level, duration: Duration): PgConnectOptions {
        val newLogSettings = logSettings.copy(
            slowStatementsLevel = level,
            slowStatementDuration = duration,
        )
        return copy(logSettings = newLogSettings)
    }

    /**
     * Return a shallow copy of the current [PgConnectOptions] with both log statement levels set
     * to [Level.OFF] and the slow statement duration set to [Duration.INFINITE].
     */
    fun disableStatementLogging(): PgConnectOptions {
        return copy(
            logSettings = LogSettings(
                statementLevel = Level.OFF,
                slowStatementsLevel = Level.OFF,
                slowStatementDuration = Duration.INFINITE
            )
        )
    }

    override fun toString(): String {
        return buildString {
            append("PgConnectOptions(host=")
            append(host)
            append(",port=")
            append(port)
            append(",username=")
            append(username)
            append(",applicationName=")
            append(applicationName)
            append(",connectionTimeout=")
            append(connectionTimeout)
            append(",password=***, database=")
            append(database)
            append(",logSettings=")
            append(logSettings)
            append(",statementCacheCapacity=")
            append(statementCacheCapacity)
            append(",extraFloatDigits=")
            append(extraFloatDigits)
            append(",sslMode=")
            append(sslMode)
            append(",currentSchema=")
            append(currentSchema)
            append(")")
        }
    }
}