package com.github.clasicrando.common

import com.github.clasicrando.common.connection.Connection
import io.github.oshai.kotlinlogging.KLogger
import io.github.oshai.kotlinlogging.KLoggingEventBuilder
import io.github.oshai.kotlinlogging.Level
import io.github.oshai.kotlinlogging.withLoggingContext
import kotlinx.coroutines.selects.SelectBuilder
import kotlinx.coroutines.selects.select

val Byte.Companion.ZERO: Byte get() = 0

inline fun <T> Result<T>.mapError(block: (Throwable) -> Throwable): Result<T> {
    if (isSuccess) {
        return this
    }
    return Result.failure(block(this.exceptionOrNull()!!))
}

sealed interface Loop {
    data object Continue : Loop
    data object Break : Loop
}

suspend inline fun selectLoop(crossinline block: SelectBuilder<Loop>.() -> Unit) {
    while (true) {
        val loop = select(builder = block)
        when (loop) {
            is Loop.Continue -> continue
            is Loop.Break -> break
        }
    }
}

fun KLogger.connectionLogger(
    connection: Connection,
    level: Level,
    block: KLoggingEventBuilder.() -> Unit,
) {
    withLoggingContext("connectionId" to connection.connectionId.toString()) {
        at(level, block = block)
    }
}
