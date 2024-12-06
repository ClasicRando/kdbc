package io.github.clasicrando.kdbc.core

import io.github.clasicrando.kdbc.core.exceptions.KdbcException
import io.github.oshai.kotlinlogging.KLogger
import io.github.oshai.kotlinlogging.KLoggingEventBuilder
import io.github.oshai.kotlinlogging.Level
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import kotlinx.io.Source
import java.io.InputStream
import kotlin.reflect.KType
import kotlin.reflect.full.withNullability
import kotlin.time.Duration

public const val ZERO_BYTE: Byte = 0

/**
 * Sealed class representing the loop control flow statements. These can be used when a lambda is
 * passed to a method that invokes the lambda within a loop. This allows the lambda to control the
 * outer loop inside nested function calls
 */
public sealed interface Loop {
    public data object Noop : Loop

    public data object Continue : Loop

    public data object Break : Loop
}

/**
 * [UniqueResourceId] extension method to log using the [logger] supplied at the specified [level]
 * using the event builder set up using the [block]. This makes each event include the
 * [resourceId][UniqueResourceId.resourceId] in each event as a key value pair.
 */
public inline fun UniqueResourceId.logWithResource(
    logger: KLogger,
    level: Level,
    crossinline block: KLoggingEventBuilder.() -> Unit,
) {
    logger.at(level) {
        block()
        val autoPayload =
            mapOf(
                "resourceId" to this@logWithResource.resourceIdAsString,
                "resourceType" to this@logWithResource.resourceType,
            )
        payload = payload?.plus(autoPayload) ?: autoPayload
    }
}

/**
 * Split the given [ByteArray] into chunks, using the definition of a C-String (null terminated
 * string). This splits by a [ZERO_BYTE] and maps each chunk into string containing each [Byte] as
 * its ascii equivalent.
 */
public fun ByteArray.splitAsCString(): List<String> {
    return this.splitBy(ZERO_BYTE)
        .map { chunk -> chunk.map { it.toInt().toChar() }.joinToString(separator = "") }
        .toList()
}

/**
 * Return a [Sequence] generator that yields 1 or more chunks of the [ByteArray], splitting by the
 * [separator] value specified.
 */
public fun ByteArray.splitBy(separator: Byte): Sequence<Sequence<Byte>> = sequence {
    var index = 0
    while (index < this@splitBy.lastIndex) {
        yield(
            generateSequence {
                if (index == this@splitBy.lastIndex) {
                    return@generateSequence null
                }
                this@splitBy[index++].takeIf { it != separator }
            }
        )
    }
}

/**
 * Call [reduceOrNull] on a [List] of [Throwable] items, aggregating to a single [Throwable] where
 * every [Throwable] after the first is added the first as a suppressed exception.
 */
public fun List<Throwable>.reduceToSingleOrNull(): Throwable? {
    if (this.isEmpty()) {
        return null
    }
    val accumulator = this[0]
    for (i in 1..<this.size) {
        accumulator.addSuppressed(this[i])
    }
    return accumulator
}

/** Wrap the [String] as if the value was a SQL identifier */
public fun String.quoteIdentifier(): String = "\"${this.replace("\"", "\"\"")}\""

public fun Duration.isZeroOrInfinite(): Boolean = this.isInfinite() || this == Duration.ZERO

/**
 * Chunk a [Flow] of [T] into a [Flow] of [List] of [T]. This collects the original [Flow] and
 * constructs a new cold flow where [List]s of the specified [size] are emitted as the flow items.
 * Every item will be the [size] specified except for the final item which will be at most the
 * [size] specified due to the dynamic size of the original [Flow].
 */
public fun <T> Flow<T>.chunked(size: Int): Flow<List<T>> = flow {
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

private const val DEFAULT_BUFFER_SIZE = 2048

/**
 * Chunk a [Source] into many [ByteArray]s with at most [size] bytes in each array. The final array
 * might have less than [size] if the total number of bytes is not equally divisible by [size].
 */
public fun Source.chunkedBytes(size: Int = DEFAULT_BUFFER_SIZE): Sequence<ByteArray> {
    return generateSequence {
        val bytes = ByteArray(size)
        when (val bytesRead = this.readAtMostTo(bytes)) {
            -1,
            0 -> null
            bytes.size -> bytes
            else -> bytes.copyOfRange(fromIndex = 0, toIndex = bytesRead)
        }
    }
}

/**
 * Chunk an [InputStream] into many [ByteArray]s with at most [size] bytes in each array. The final
 * array might have less than [size] if the total number of bytes is not equally divisible by
 * [size].
 */
public fun InputStream.chunkedBytes(size: Int = DEFAULT_BUFFER_SIZE): Sequence<ByteArray> {
    return generateSequence {
        val bytes = ByteArray(size)
        when (val bytesRead = this.read(bytes)) {
            -1,
            0 -> null
            bytes.size -> bytes
            else -> bytes.copyOfRange(fromIndex = 0, toIndex = bytesRead)
        }
    }
}

/**
 * Utility method to replace all whitespace 1 or more times with a single space.
 *
 * Equivalent to
 *
 * ```
 * string.replace(Regex("\\s+"), "")
 * ```
 */
public fun String.normalizeWhitespace(): String = this.replace(Regex("\\s+"), " ")

public const val DEFAULT_KDBC_TEST_TIMEOUT: Long = 60L

/**
 * Returns this [KType] if it's non-null or a new non-null version of this [KType] if it's nullable
 */
@Suppress("NOTHING_TO_INLINE")
public inline fun KType.ensureNonNull(): KType {
    return if (this.isMarkedNullable) {
        this.withNullability(nullable = false)
    } else {
        this
    }
}

public fun splitQuery(query: String): List<String> = buildList {
    if (!query.contains(';')) {
        add(query)
        return@buildList
    }
    val builder = StringBuilder()
    var inQuote = false
    val iter = query.iterator()
    while (iter.hasNext()) {
        when (val char = iter.nextChar()) {
            '\'' -> {
                inQuote = !inQuote
                builder.append(char)
            }
            ';' ->
                if (inQuote) {
                    builder.append(char)
                } else {
                    add(builder.toString())
                    builder.clear()
                }
            else -> builder.append(char)
        }
    }
    if (builder.isNotEmpty()) {
        add(builder.toString())
    }
}

public fun validateByte(value: Long): Byte {
    if (value < Byte.MIN_VALUE || value > Byte.MAX_VALUE) {
        throw KdbcException(
            "Invalid TINYINT value. $value must be between ${Byte.MIN_VALUE} and ${Byte.MAX_VALUE}"
        )
    }
    return value.toByte()
}

public fun validateShort(value: Long): Short {
    if (value < Short.MIN_VALUE || value > Short.MAX_VALUE) {
        throw KdbcException(
            "Invalid SMALLINT value. $value must be between ${Short.MIN_VALUE} and ${Short.MAX_VALUE}"
        )
    }
    return value.toShort()
}

public fun validateInt(value: Long): Int {
    if (value < Int.MIN_VALUE || value > Int.MAX_VALUE) {
        throw KdbcException(
            "Invalid INT value. $value must be between ${Int.MIN_VALUE} and ${Int.MAX_VALUE}"
        )
    }
    return value.toInt()
}

private val nullStringBuilder = StringBuilder("\\N")

private fun StringBuilder.buildOrNull(): String? {
    if (this.compareTo(nullStringBuilder) == 0) {
        return null
    }
    return this.toString()
}

/**
 * Accepts a CSV row as [bytes] and the [expectedColumnCount] to parse the rows as an [Array] of
 * nullable [String]s. In this context, null is a '\N' string and the newline character is always
 * '\n'.
 */
public fun parseBytesAsCsvRow(bytes: ByteArray, expectedColumnCount: Int): Array<String?> {
    val output = arrayOfNulls<String?>(expectedColumnCount)
    val row = bytes.toString(charset = Charsets.UTF_8)
    val charIter = row.iterator()

    var index = 0
    var lastChar = '\u0000'
    val builder = StringBuilder()
    var inQuote = false
    while (charIter.hasNext()) {
        val currentChar = charIter.next()
        when (currentChar) {
            ',' -> {
                if (inQuote) {
                    builder.append(currentChar)
                } else {
                    output[index++] = builder.buildOrNull()
                    builder.clear()
                }
            }
            '"' -> {
                if (lastChar == '"') {
                    builder.append(currentChar)
                    inQuote = true
                } else {
                    inQuote = !inQuote
                }
            }
            '\n' -> {
                if (inQuote) {
                    builder.append(currentChar)
                } else {
                    output[index++] = builder.buildOrNull()
                    break
                }
            }
            else -> {
                builder.append(currentChar)
            }
        }
        lastChar = currentChar
    }
    return output
}
