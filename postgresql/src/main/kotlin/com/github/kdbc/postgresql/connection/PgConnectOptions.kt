package com.github.kdbc.postgresql.connection

import com.github.kdbc.core.LogSettings
import com.github.kdbc.core.SslMode
import com.github.kdbc.core.pool.PoolOptions
import com.github.kdbc.postgresql.CertificateInput
import io.github.oshai.kotlinlogging.Level
import kotlinx.serialization.Serializable
import kotlin.time.Duration
import kotlin.time.DurationUnit
import kotlin.time.toDuration

/** Connection options for a postgresql database */
@Serializable
data class PgConnectOptions(
    /** Host name or IP address of the postgresql server */
    val host: String,
    /** Port on the host machine of the postgresql server */
    val port: UShort,
    /** Name of the user to log in to the postgresql server */
    val username: String,
    /** Optional application name to set as part of the connection context */
    val applicationName: String? = null,
    /** Timeout duration during initial TCP connection establishment */
    val connectionTimeout: Duration = 100.toDuration(DurationUnit.MILLISECONDS),
    /** Password if the database instance requires a password */
    val password: String? = null,
    /**
     * Optional initial connection database name. If not specified then postgresql will assume a
     * database with the same name as the [username]
     */
    val database: String? = null,
    /** Statement logging settings. If not specified, [LogSettings.DEFAULT] is used. */
    val logSettings: LogSettings = LogSettings.DEFAULT,
    /** Size of the cache storing prepared statement on the client side */
    val statementCacheCapacity: UShort = 100U,
    /**  */
    val useExtendedProtocolForSimpleQueries: Boolean = true,
    /**
     * This parameter adjusts the number of digits used for textual output of floating-point values,
     * including float4, float8, and geometric data types. Default is 1
     *
     * [docs](https://www.postgresql.org/docs/16/runtime-config-client.html#GUC-EXTRA-FLOAT-DIGITS)
     */
    val extraFloatDigits: Int = 1,
    /**
     * SSL Mode of the connection. Currently, does nothing until SSL connections are implemented.
     */
    val sslMode: SslMode = SslMode.DEFAULT,
    /**
     * SSL root certificate of the connection. Currently, does nothing until SSL connections are
     * implemented.
     */
    val sslRootCert: CertificateInput? = null,
    /**
     * SSL client certificate of the connection. Currently, does nothing until SSL connections are
     * implemented.
     */
    val sslClientCert: CertificateInput? = null,
    /**
     * SSL client key of the connection. Currently, does nothing until SSL connections are
     * implemented.
     */
    val sslClientKey: CertificateInput? = null,
    /**
     * The default schema within the database connection. Sets the `search_path` connection
     * parameter. When null specified (the default) then the default connection property is used
     * which is public.
     */
    val currentSchema: String? = null,
    /**
     * If the connection should perform a rollback command automatically if server ends a query
     * response with an indicator that the current transaction failed. Default is true.
     */
    val autoRollbackOnFailedTransaction: Boolean = true,
    /**
     * [PoolOptions] for the database connection. Used to decide how connections to the database
     * using these options should pool connections behind the scenes. If not specified then the
     * default values for a [PoolOptions] is used.
     *
     * @see PoolOptions
     */
    val poolOptions: PoolOptions = PoolOptions(),
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
     * to [Level.TRACE] and the slow statement duration set to [Duration.INFINITE].
     */
    fun disableStatementLogging(): PgConnectOptions {
        return copy(
            logSettings = LogSettings(
                statementLevel = Level.TRACE,
                slowStatementsLevel = Level.TRACE,
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
            append(",poolOptions=")
            append(poolOptions)
            append(")")
        }
    }
}