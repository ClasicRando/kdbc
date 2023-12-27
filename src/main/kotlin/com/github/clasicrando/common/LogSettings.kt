package com.github.clasicrando.common

import io.github.oshai.kotlinlogging.Level
import kotlin.time.Duration
import kotlin.time.DurationUnit
import kotlin.time.toDuration

data class LogSettings(
    val statementLevel: Level,
    val slowStatementsLevel: Level,
    val slowStatementDuration: Duration,
) {
    companion object {
        val DEFAULT = LogSettings(
            statementLevel = Level.DEBUG,
            slowStatementsLevel = Level.WARN,
            slowStatementDuration = 1.toDuration(DurationUnit.SECONDS),
        )
    }
}
