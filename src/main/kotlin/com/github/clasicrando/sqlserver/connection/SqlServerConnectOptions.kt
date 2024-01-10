package com.github.clasicrando.sqlserver.connection

import com.github.clasicrando.common.LogSettings
import com.github.clasicrando.common.connection.ConnectOptions
import com.github.clasicrando.sqlserver.authentication.AuthenticationMethod
import io.github.oshai.kotlinlogging.Level
import kotlin.time.Duration

data class SqlServerConnectOptions(
    val host: String? = null,
    val port: Short? = null,
    val database: String? = null,
    val instanceName: String? = null,
    val applicationName: String? = null,
    val encryption: EncryptionLevel,
    val trust: TrustConfiguration,
    val auth: AuthenticationMethod,
    val readonly: Boolean = false,
    val logSettings: LogSettings = LogSettings.DEFAULT,
) : ConnectOptions {
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
