package com.github.clasicrando.common

import com.github.clasicrando.common.connection.Connection
import io.github.oshai.kotlinlogging.KLogger
import io.github.oshai.kotlinlogging.KLoggingEventBuilder
import io.github.oshai.kotlinlogging.Level
import kotlinx.coroutines.channels.ReceiveChannel
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

suspend inline fun waitOrError(
    errorChannel: ReceiveChannel<Throwable>,
    vararg otherChannels: ReceiveChannel<Any>,
) {
    select {
        errorChannel.onReceive {
            throw it
        }
        for (channel in otherChannels) {
            channel.onReceive {}
        }
    }
}

inline fun KLogger.connectionLogger(
    connection: Connection,
    level: Level,
    crossinline block: KLoggingEventBuilder.() -> Unit,
) {
    val connectionId = "connectionId" to connection.connectionId.toString()
    at(level) {
        block()
        payload = payload?.plus(connectionId) ?: mapOf(connectionId)
    }
}

fun ByteArray.splitAsCString(): List<String> {
    return this.splitBy(Byte.ZERO)
        .map { chunk ->
            chunk.map { it.toInt().toChar() }
                .joinToString(separator = "")
        }
        .toList()
}

fun ByteArray.splitBy(byte: Byte): Sequence<Sequence<Byte>> = sequence {
    var index = 0
    while (index < this@splitBy.lastIndex) {
        yield(generateSequence {
            if (index == this@splitBy.lastIndex) {
                return@generateSequence null
            }
            this@splitBy[index++].takeIf { it != byte }
        })
    }
}

fun List<Throwable>.reduceToSingleOrNull(): Throwable? {
    return this.reduceOrNull { acc, throwable ->
        acc.addSuppressed(throwable)
        acc
    }
}

fun String.quoteIdentifier(): String = "\"${this.replace("\"", "\"\"")}\""
