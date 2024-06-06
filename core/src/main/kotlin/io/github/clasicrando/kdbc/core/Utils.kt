package io.github.clasicrando.kdbc.core

import io.github.oshai.kotlinlogging.KLogger
import io.github.oshai.kotlinlogging.KLoggingEventBuilder
import io.github.oshai.kotlinlogging.Level
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
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
 * [UniqueResourceId] extension method to log using the [logger] supplied at the specified [level]
 * using the event builder set up using the [block]. This makes each event include the
 * [resourceId][UniqueResourceId.resourceId] in each event as a key value pair.
 */
inline fun UniqueResourceId.logWithResource(
    logger: KLogger,
    level: Level,
    crossinline block: KLoggingEventBuilder.() -> Unit,
) {
    logger.at(level) {
        block()
        val autoPayload = mapOf(
            "resourceId" to this@logWithResource.resourceIdAsString,
            "resourceType" to this@logWithResource.resourceType,
        )
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

/**
 * Chunk a [Flow] of [T] into a [Flow] of [List] of [T]. This collects the original [Flow] and
 * constructs a new cold flow where [List]s of the specified [size] are emitted as the flow items.
 * Every item will be the [size] specified except for the final item which will be at most the
 * [size] specified due to the dynamic size of the original [Flow].
 */
fun <T> Flow<T>.chunked(size: Int): Flow<List<T>> {
    return flow {
        val buffer = ArrayList<T>(size)
        this@chunked.collect {
            buffer.add(it)
            if (buffer.size == size) {
                emit(buffer)
                buffer.clear()
            }
        }
        if (buffer.isNotEmpty()) {
            emit(buffer)
        }
    }
}
