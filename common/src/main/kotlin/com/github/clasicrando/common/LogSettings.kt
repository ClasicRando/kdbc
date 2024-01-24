package com.github.clasicrando.common

import io.github.oshai.kotlinlogging.Level
import kotlin.time.Duration
import kotlin.time.DurationUnit
import kotlin.time.toDuration

/**
 * Settings for logging statements internally. Allow for users to bring up internal logging
 * statements if they are required to be in the user's logs without lowering their threshold.
 */
data class LogSettings(
    /** Log level for logging requests to execute statements */
    val statementLevel: Level,
    /**
     * Log level to report when a single statement took too long to run. Currently, has no effect
     * on internal logging.
     */
    val slowStatementsLevel: Level,
    /**
     * [Duration] at which statements are logged as slow. Currently, has no effect on internal
     * logging.
     */
    val slowStatementDuration: Duration,
) {
    companion object {
        val DEFAULT = com.github.clasicrando.common.LogSettings(
            statementLevel = Level.DEBUG,
            slowStatementsLevel = Level.WARN,
            slowStatementDuration = 1.toDuration(DurationUnit.SECONDS),
        )
    }
}
