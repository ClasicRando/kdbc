package io.github.clasicrando.kdbc.mysql.connection

import io.github.clasicrando.kdbc.core.SslMode
import io.github.oshai.kotlinlogging.Level
import io.ktor.network.tls.TLSConfigBuilder
import kotlinx.serialization.Serializable
import kotlinx.serialization.Transient
import java.time.ZoneOffset
import kotlin.time.Duration
import kotlin.time.DurationUnit
import kotlin.time.toDuration

/** Connection options for a mysql database */
@Serializable
public data class MySqlConnectionOptions(
    /** Host name or IP address of the postgresql server */
    val host: String,
    /** Port on the host machine of the postgresql server */
    val port: Int,
    /** Name of the user to log in to the postgresql server */
    val username: String,
    /**
     * Optional application name to set as part of the connection context. Defaults to
     * 'kdbc-mysql-application'
     */
    val applicationName: String = "kdbc-mysql-application",
    /** Timeout duration during initial TCP connection establishment */
    val connectionTimeout: Duration = 10.toDuration(DurationUnit.SECONDS),
    /** Password if the database instance requires a password */
    val password: String? = null,
    /**
     * Optional initial connection database name. If not specified then postgresql will assume a
     * database with the same name as the [username]
     */
    val database: String? = null,
    /** Statement logging level. If not specified, [Level.DEBUG] is used. */
    val statementLogLevel: Level = Level.DEBUG,
    /**
     * The duration that is waited before canceling a query execution due to timeout. The default is
     * an infinite timeout which means it will never cancel a query.
     *
     * This applies the `max_execution_time` variable to the current session so every `SELECT` query
     * that doesn't modify the database or override with a specific `MAX_EXECUTION_TIME(N)` query
     * hint will be terminated after the timeout is reached. Therefore, this has no impact on DML
     * statement or `SELECT` statements within stored procedures/functions.
     *
     * Note: [Duration.ZERO] is also treated as infinite and negative timeouts are ignored with the
     * default value is used
     */
    val queryTimeout: Duration = Duration.INFINITE,
    /** Size of the cache storing prepared statement on the client side */
    val statementCacheCapacity: Int = 100,
    /** SSL Mode of the connection. Uses the default mode of [SslMode.Prefer] */
    val sslMode: SslMode = SslMode.DEFAULT,
    /** Allow for connection to database with clear text auth plugin */
    val allowClearTextPlugin: Boolean = false,
    /**
     * Flag that enables or disables the `NO_ENGINE_SUBSTITUTION` sql_mod settings after connection.
     *
     * If not set to true and the available storage engine specified by a `CREATE TABLE` is not
     * available, a warning is given and the default storage engine is used instead. If set true,
     * engine substitution is forbidden and will result in an error.
     */
    val noEngineSubstitution: Boolean = true,
    /**
     * Allows a client to take a batch of duplicate `INSERT` statements and rewrite to a single
     * `INSERT` statement with chained `VALUES` tuples to execute all inserts in 1 statement. This
     * override the `withTransaction` parameter supplied to a query batch execution since all
     * inserts work or the entire statement fails and no records are inserted.
     */
    val rewriteBatchInsertQuery: Boolean = false,
    /**
     * Timezone offset to use when retrieving timezone aware types from the database. The connection
     * itself will always use UTC but when [java.time.OffsetDateTime] is requested, this offset will
     * be applied before returning the value. The default value is [ZoneOffset.UTC].
     */
    @Transient val timeZoneOffset: ZoneOffset = ZoneOffset.UTC,
    /**
     * TLS Config builder action to modify the config provided to the ktor socket creator. This is
     * only used if the server supports TLS and the socket used to create the database connection is
     * a [io.github.clasicrando.kdbc.core.stream.KtorStream] (i.e. only for async connections).
     */
    @Transient val tlsConfig: TLSConfigBuilder.() -> Unit = {},
) {
    /** Connection properties as they are sent to the database upon connection initialization */
    @Transient
    val properties: Map<String, String> =
        mapOf(
            "_client_name" to "kdbc-mysql-driver",
            "_client_version" to "0.0.4",
            "_program_name" to applicationName,
        )

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as MySqlConnectionOptions

        if (port != other.port) return false
        if (statementCacheCapacity != other.statementCacheCapacity) return false
        if (allowClearTextPlugin != other.allowClearTextPlugin) return false
        if (noEngineSubstitution != other.noEngineSubstitution) return false
        if (rewriteBatchInsertQuery != other.rewriteBatchInsertQuery) return false
        if (host != other.host) return false
        if (username != other.username) return false
        if (applicationName != other.applicationName) return false
        if (connectionTimeout != other.connectionTimeout) return false
        if (database != other.database) return false
        if (statementLogLevel != other.statementLogLevel) return false
        if (queryTimeout != other.queryTimeout) return false
        if (sslMode != other.sslMode) return false
        if (timeZoneOffset != other.timeZoneOffset) return false

        return true
    }

    override fun hashCode(): Int {
        var result = port
        result = 31 * result + statementCacheCapacity
        result = 31 * result + allowClearTextPlugin.hashCode()
        result = 31 * result + noEngineSubstitution.hashCode()
        result = 31 * result + rewriteBatchInsertQuery.hashCode()
        result = 31 * result + host.hashCode()
        result = 31 * result + username.hashCode()
        result = 31 * result + applicationName.hashCode()
        result = 31 * result + connectionTimeout.hashCode()
        result = 31 * result + (database?.hashCode() ?: 0)
        result = 31 * result + statementLogLevel.hashCode()
        result = 31 * result + queryTimeout.hashCode()
        result = 31 * result + sslMode.hashCode()
        result = 31 * result + timeZoneOffset.hashCode()
        return result
    }

    override fun toString(): String {
        return buildString {
            append("MySqlConnectionOptions(host='")
            append(host)
            append("', port=")
            append(port)
            append(", username='")
            append(username)
            append("', applicationName='")
            append(applicationName)
            append("', connectionTimeout=")
            append(connectionTimeout)
            append(", database=")
            append(database)
            append(", statementLogLevel=")
            append(statementLogLevel)
            append(", queryTimeout=")
            append(queryTimeout)
            append(", statementCacheCapacity=")
            append(statementCacheCapacity)
            append(", sslMode=")
            append(sslMode)
            append(", allowClearTextPlugin=")
            append(allowClearTextPlugin)
            append(", noEngineSubstitution=")
            append(noEngineSubstitution)
            append(", rewriteBatchInsertQuery=")
            append(rewriteBatchInsertQuery)
            append(", timeZoneOffset=")
            append(timeZoneOffset)
            append(")")
        }
    }
}
