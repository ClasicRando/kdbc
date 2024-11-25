package io.github.clasicrando.kdbc.core

import com.ionspin.kotlin.bignum.decimal.BigDecimal
import com.ionspin.kotlin.bignum.integer.BigInteger
import com.ionspin.kotlin.bignum.integer.Sign
import io.github.clasicrando.kdbc.core.exceptions.KdbcException
import io.github.oshai.kotlinlogging.KLogger
import io.github.oshai.kotlinlogging.KLoggingEventBuilder
import io.github.oshai.kotlinlogging.Level
import java.io.InputStream
import kotlin.reflect.KType
import kotlin.reflect.full.withNullability
import kotlin.time.Duration
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import kotlinx.io.Source

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
 * Get the traditional scale of the [BigDecimal] by taking the number of digits after the decimal
 * place (in the simplified representation of the [BigDecimal]) and subtracting the
 * [BigDecimal.exponent] value.
 *
 * For example, if your [BigDecimal] is "25345.5265" then the simplified representation will be
 * "2.53455265E+4" so your number of digits after the decimal place are 8 and your exponent is 4
 * which calculates the traditional scale as 4 (i.e. the number of digits after the true decimal
 * place).
 */
public inline val BigDecimal.traditionalScale: Long
    get() = significand.numberOfDecimalDigits() - 1 - exponent

/**
 * Construct a new [BigDecimal] from this [BigInteger] by calculating the expected simplified
 * representation exponent as the number of digits after the simplified representations decimal
 * place minus the specified traditional [scale].
 *
 * For example, if your [BigInteger] is "253455265" and your scale is 4, then the common
 * representation is "25345.5265" which means the simplified representation is "2.53455265E+4". This
 * is calculated because the number of digits after the eventual simplified representation is 8 and
 * the [scale] is 4 so the effective exponent in the simplified representation is 4.
 */
public fun BigInteger.toBigDecimalWithTraditionalScale(scale: Short): BigDecimal {
    return BigDecimal.fromBigIntegerWithExponent(
        bigInteger = this,
        exponent = this.numberOfDecimalDigits() - 1 - scale,
    )
}

/**
 * Utility method to convert a [java.math.BigInteger] to a BigNum [BigInteger].
 *
 * Uses the [java.math.BigInteger.toByteArray] method to get the raw data of the integer value and
 * use that along with the [java.math.BigInteger.signum] value to construct a [BigInteger].
 */
public fun java.math.BigInteger.toBigNum(): BigInteger {
    return BigInteger.fromByteArray(
        this.toByteArray(),
        when (val sigNum = this.signum()) {
            -1 -> Sign.NEGATIVE
            0 -> Sign.ZERO
            1 -> Sign.POSITIVE
            else -> error("Unexpected BigInteger.signum(). Expected -1..1, found $sigNum")
        },
    )
}

/**
 * Utility method to convert a [java.math.BigDecimal] to a BigNum [BigDecimal].
 *
 * Uses [toBigNum] to convert the unscaled version of this decimal value to a [BigInteger], the uses
 * the scale to call [toBigDecimalWithTraditionalScale].
 */
public fun java.math.BigDecimal.toBigNum(): BigDecimal {
    return this.unscaledValue().toBigNum().toBigDecimalWithTraditionalScale(this.scale().toShort())
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
        throw KdbcException("Invalid TINYINT value. $value must be between ${Byte.MIN_VALUE} and ${Byte.MAX_VALUE}")
    }
    return value.toByte()
}

public fun validateShort(value: Long): Short {
    if (value < Short.MIN_VALUE || value > Short.MAX_VALUE) {
        throw KdbcException("Invalid SMALLINT value. $value must be between ${Short.MIN_VALUE} and ${Short.MAX_VALUE}")
    }
    return value.toShort()
}

public fun validateInt(value: Long): Int {
    if (value < Int.MIN_VALUE || value > Int.MAX_VALUE) {
        throw KdbcException("Invalid INT value. $value must be between ${Int.MIN_VALUE} and ${Int.MAX_VALUE}")
    }
    return value.toInt()
}
