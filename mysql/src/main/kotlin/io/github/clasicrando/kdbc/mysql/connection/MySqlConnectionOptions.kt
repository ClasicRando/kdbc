package io.github.clasicrando.kdbc.mysql.connection

import io.github.clasicrando.kdbc.core.LogSettings
import io.github.clasicrando.kdbc.core.SslMode
import io.github.clasicrando.kdbc.core.isZeroOrInfinite
import io.ktor.network.tls.TLSConfigBuilder
import kotlinx.serialization.Serializable
import kotlinx.serialization.Transient
import kotlin.time.Duration
import kotlin.time.DurationUnit
import kotlin.time.toDuration

/** Connection options for a mysql database */
@Serializable
data class MySqlConnectionOptions(
    /** Host name or IP address of the postgresql server */
    val host: String,
    /** Port on the host machine of the postgresql server */
    val port: Int,
    /** Name of the user to log in to the postgresql server */
    val username: String,
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
     * SSL Mode of the connection. Uses the default mode of [SslMode.Prefer]
     */
    val sslMode: SslMode = SslMode.DEFAULT,
    /**
     * Allow for connection to database with clear text auth plugin
     */
    val allowClearTextPlugin: Boolean = false,
    /**
     * Flag that enables or disables the `NO_ENGINE_SUBSTITUTION` sql_mod settings after connection.
     *
     * If not set to true and the available storage engine specified by a `CREATE TABLE` is not
     * available, a warning is given and the default storage engine is used instead. If set true,
     * engine substitution is forbidden and will result in an error.
     */
    val noEngineSubstitution: Boolean = true,
    val charset: Charset,
    val collation: Collation?,
    /**
     * TLS Config builder action to modify the config provided to the ktor socket creator. This is
     * only used if the server supports TLS and the socket used to create the database connection
     * is a [io.github.clasicrando.kdbc.core.stream.KtorStream] (i.e. only for async
     * connections).
     */
    @Transient
    val tlsConfig: TLSConfigBuilder.() -> Unit = {},
) {
    /** Connection properties as they are sent to the database upon connection initialization */
    @Transient
    val properties: List<Pair<String, String>> =
        listOf(
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
            "statement_timeout" to
                queryTimeout.coerceAtLeast(Duration.ZERO).let {
                    if (it.isZeroOrInfinite()) {
                        "0"
                    } else {
                        it.inWholeMilliseconds.coerceAtMost(Int.MAX_VALUE.toLong()).toString()
                    }
                },
        ).mapNotNull { (key, value) ->
            value?.let { key to it }
        }
}
