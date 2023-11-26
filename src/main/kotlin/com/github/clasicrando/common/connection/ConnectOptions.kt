package com.github.clasicrando.common.connection

import io.klogging.Level
import kotlin.time.Duration

interface ConnectOptions {
    fun logStatements(level: Level): ConnectOptions
    fun logSlowStatements(level: Level, duration: Duration): ConnectOptions
    fun disableStatementLogging(): ConnectOptions {
        return logStatements(Level.NONE)
            .logSlowStatements(Level.NONE, Duration.ZERO)
    }
}
