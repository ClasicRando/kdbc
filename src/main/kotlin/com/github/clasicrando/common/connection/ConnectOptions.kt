package com.github.clasicrando.common.connection

import io.github.oshai.kotlinlogging.Level
import kotlin.time.Duration

/**
 * Interface for all connection options to enable users to alter the statement logging behaviour
 * of all [Connection] implementations.
 */
interface ConnectOptions {
    /**
     * Return a shallow copy of the current [ConnectOptions] with the log statement [level] altered
     */
    fun logStatements(level: Level): ConnectOptions
    /**
     * Return a shallow copy of the current [ConnectOptions] with the new log slow statement
     * [level] and [duration] altered
     */
    fun logSlowStatements(level: Level, duration: Duration): ConnectOptions
}

/**
 * Return a shallow copy of the current [ConnectOptions] with both log statement levels set to
 * [Level.TRACE] and the slow statement duration set to [Duration.INFINITE]
 */
fun ConnectOptions.disableStatementLogging(): ConnectOptions {
    return logStatements(Level.TRACE)
        .logSlowStatements(Level.TRACE, Duration.INFINITE)
}
