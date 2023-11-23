package com.github.clasicrando.postgresql

import com.github.clasicrando.common.ConnectOptions
import com.github.clasicrando.common.SslMode
import io.klogging.Level
import io.ktor.utils.io.charsets.Charset
import kotlinx.io.files.Path
import kotlin.time.Duration

data class PgConnectOptions(
    val host: String,
    val port: UShort,
    val username: String,
    val connectionTimeout: ULong,
    val applicationName: String,
    val password: String? = null,
    val database: String? = null,
    val logSettings: LogSettings = LogSettings.DEFAULT,
    val statementCacheCapacity: ULong = 100U,
    val charset: Charset = Charsets.UTF_8,
    val extraFloatDigits: String = "2",
    val sslMode: SslMode = SslMode.Disable,
    val sslRootCert: CertificateInput? = null,
    val sslClientCert: CertificateInput? = null,
    val sslClientKey: CertificateInput? = null,
    val socket: Path? = null,
    val currentSchema: String? = null,
    val options: String? = null,
) : ConnectOptions {
    val properties: List<Pair<String, String>> = listOf(
        "user" to username,
        "database" to database,
        "client_encoding" to charset.name(),
        "DateStyle" to "ISO",
        "intervalstyle" to "iso_8601",
        "TimeZone" to "UTC",
        "extra_float_digits" to extraFloatDigits,
        "search_path" to currentSchema,
    ).mapNotNull { (key, value) ->
        value?.let { key to it }
    }

    override fun logStatements(level: Level): ConnectOptions {
        val newLogSettings = logSettings.copy(statementLevel = level)
        return copy(logSettings = newLogSettings)
    }

    override fun logSlowStatements(level: Level, duration: Duration): ConnectOptions {
        val newLogSettings = logSettings.copy(
            slowStatementsLevel = level,
            slowStatementDuration = duration,
        )
        return copy(logSettings = newLogSettings)
    }
}
