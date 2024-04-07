package com.github.kdbc.core

import io.github.oshai.kotlinlogging.KLogger
import io.github.oshai.kotlinlogging.KLoggingEventBuilder
import io.github.oshai.kotlinlogging.Level
import kotlin.time.Duration

const val zeroByte: Byte = 0

/**
 * Sealed class representing the loop control flow statements. These can be used when a lambda
 * is passed to a method that invokes the lambda within a loop. This allows the lambda to control
 * the outer loop inside nested function calls
 */
sealed interface Loop {
    data object Noop: Loop
    data object Continue : Loop
    data object Break : Loop
}

/**
 * [KLogger] extension method to log at the specified [level] using the event builder set up using
 * the [block] within the context of the [resource] provided. This makes each event include the
 * [resourceId][UniqueResourceId.resourceId] in each event as a key value pair.
 */
inline fun KLogger.resourceLogger(
    resource: UniqueResourceId,
    level: Level,
    crossinline block: KLoggingEventBuilder.() -> Unit,
) {
    val autoPayload = mapOf(
        "resourceId" to resource.resourceIdAsString,
        "resourceType" to resource.resourceType,
    )
    at(level) {
        block()
        payload = payload?.plus(autoPayload) ?: autoPayload
    }
}

/**
 * Split the given [ByteArray] into chunks, using the definition of a C-String (null terminated
 * string). This splits by a [zeroByte] and maps each chunk into string containing each [Byte] as
 * its ascii equivalent.
 */
fun ByteArray.splitAsCString(): List<String> {
    return this.splitBy(zeroByte)
        .map { chunk ->
            chunk.map { it.toInt().toChar() }
                .joinToString(separator = "")
        }
        .toList()
}

/**
 * Return a [Sequence] generator that yields 1 or more chunks of the [ByteArray], splitting by the
 * [separator] value specified.
 */
fun ByteArray.splitBy(separator: Byte): Sequence<Sequence<Byte>> = sequence {
    var index = 0
    while (index < this@splitBy.lastIndex) {
        yield(generateSequence {
            if (index == this@splitBy.lastIndex) {
                return@generateSequence null
            }
            this@splitBy[index++].takeIf { it != separator }
        })
    }
}

/**
 * Call [reduceOrNull] on a [List] of [Throwable] items, aggregating to a single [Throwable] where
 * every [Throwable] after the first is added the first as a suppressed exception.
 */
fun List<Throwable>.reduceToSingleOrNull(): Throwable? {
    return this.reduceOrNull { acc, throwable ->
        acc.addSuppressed(throwable)
        acc
    }
}

/** Wrap the [String] as if the value was a SQL identifier */
fun String.quoteIdentifier(): String = "\"${this.replace("\"", "\"\"")}\""

fun Duration.isZeroOrInfinite(): Boolean = this.isInfinite() || this == Duration.ZERO
