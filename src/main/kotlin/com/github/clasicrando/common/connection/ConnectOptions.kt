package com.github.clasicrando.common.connection

import io.github.oshai.kotlinlogging.Level
import kotlin.time.Duration

interface ConnectOptions {
    fun logStatements(level: Level): ConnectOptions
    fun logSlowStatements(level: Level, duration: Duration): ConnectOptions
    fun disableStatementLogging(): ConnectOptions {
        return logStatements(Level.TRACE)
            .logSlowStatements(Level.TRACE, Duration.ZERO)
    }
}
