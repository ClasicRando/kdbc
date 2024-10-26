package io.github.clasicrando.kdbc.postgresql.type

import io.github.clasicrando.kdbc.core.buffer.ByteWriteBuffer
import io.github.clasicrando.kdbc.postgresql.column.PgValue
import kotlinx.datetime.DateTimePeriod
import kotlin.reflect.typeOf
import kotlin.time.Duration

private const val MINUTES_PER_HOUR = 60L
private const val SECONDS_PER_MINUTE = 60L
private const val MICROSECONDS_PER_SECOND = 1_000_000L
private const val MICROSECONDS_PER_MILLISECOND = 1_000L
private const val NANOSECONDS_PER_MICROSECOND = 1_000.0
private const val MICROSECONDS_PER_MINUTE = SECONDS_PER_MINUTE * MICROSECONDS_PER_SECOND
private const val MICROSECONDS_PER_HOUR = MINUTES_PER_HOUR * MICROSECONDS_PER_MINUTE

/**
 * Extract the number of whole microseconds from this [DateTimePeriod] rounding down for fractional
 * nanoseconds present.
 */
private fun DateTimePeriod.inWholeMicroSeconds(): Long =
    this.hours * MICROSECONDS_PER_HOUR +
        this.minutes * MICROSECONDS_PER_MINUTE +
        this.seconds * MICROSECONDS_PER_SECOND +
        kotlin.math.floor(this.nanoseconds / NANOSECONDS_PER_MICROSECOND).toLong()

internal object DateTimePeriodTypeDescription : PgTypeDescription<DateTimePeriod>(
    dbType = PgType.Interval,
    kType = typeOf<DateTimePeriod>(),
) {
    /**
     * Writes 3 values of the [DateTimePeriod] to represent the interval:
     * 1. [Long] - whole micro seconds of the time portion
     * 2. [Int] - number of days
     * 3. [Int] - number of total months
     *
     * [pg source code](https://github.com/postgres/postgres/blob/874d817baa160ca7e68bee6ccc9fc1848c56e750/src/backend/utils/adt/timestamp.c#L1007)
     */
    override fun encode(
        value: DateTimePeriod,
        buffer: ByteWriteBuffer,
    ) {
        buffer.writeLong(value.inWholeMicroSeconds())
        buffer.writeInt(value.days)
        buffer.writeInt(value.years * 12 + value.months)
    }

    /**
     * Reads 3 values that represent the interval and pack them into a new [DateTimePeriod].
     *
     * 1. [Long] - whole micro seconds of the time portion
     * 2. [Int] - number of days
     * 3. [Int] - number of total months
     *
     * [pg source code](https://github.com/postgres/postgres/blob/874d817baa160ca7e68bee6ccc9fc1848c56e750/src/backend/utils/adt/timestamp.c#L1032)
     */
    override fun decodeBytes(value: PgValue.Binary): DateTimePeriod {
        val microSeconds = value.bytes.readLong()
        val days = value.bytes.readInt()
        val months = value.bytes.readInt()
        return DateTimePeriod(months = months, days = days, nanoseconds = microSeconds * 1000)
    }

    /**
     * Attempt to parse the [String] into a [DateTimePeriod]. The expected format is ISO-8601 (this
     * is the interval format specified when connecting to the database).
     *
     * [pg source code](https://github.com/postgres/postgres/blob/874d817baa160ca7e68bee6ccc9fc1848c56e750/src/backend/utils/adt/timestamp.c#L983)
     */
    override fun decodeText(value: PgValue.Text): DateTimePeriod = DateTimePeriod.parse(value.text)
}

/**
 * Custom postgres `interval` type for use instead of [DateTimePeriod]. Can be created from a
 * [Duration] but some precision loss and overflow can occur.
 */
data class PgInterval(
    val months: Int,
    val days: Int,
    val microseconds: Long,
)

/**
 * Convert the duration to a postgres `interval`. Some precision is loss during the conversion if
 * nanoseconds are present. Max precision is microseconds.
 *
 * @throws IllegalStateException if the number of days exceeds [Int.MAX_VALUE]
 */
fun Duration.toPgInterval(): PgInterval =
    this.toComponents { days, hours, minutes, seconds, nanoseconds ->
        check(days <= Int.MAX_VALUE) { "Number of days cannot exceed ${Int.MAX_VALUE}" }
        PgInterval(
            months = 0,
            days = days.toInt(),
            microseconds =
                hours * MICROSECONDS_PER_HOUR +
                    minutes * MICROSECONDS_PER_MINUTE +
                    seconds * MICROSECONDS_PER_SECOND +
                    kotlin.math.floor(nanoseconds / NANOSECONDS_PER_MICROSECOND).toLong(),
        )
    }

internal object PgIntervalTypeDescription : PgTypeDescription<PgInterval>(
    dbType = PgType.Interval,
    kType = typeOf<PgInterval>(),
) {
    /**
     * Writes 3 values of the [DateTimePeriod] to represent the interval:
     * 1. [Long] - whole micro seconds of the time portion
     * 2. [Int] - number of days
     * 3. [Int] - number of total months
     *
     * [pg source code](https://github.com/postgres/postgres/blob/874d817baa160ca7e68bee6ccc9fc1848c56e750/src/backend/utils/adt/timestamp.c#L1007)
     */
    override fun encode(
        value: PgInterval,
        buffer: ByteWriteBuffer,
    ) {
        buffer.writeLong(value.microseconds)
        buffer.writeInt(value.days)
        buffer.writeInt(value.months)
    }

    /**
     * Reads 3 values that represent the interval and pack them into a new [DateTimePeriod].
     *
     * 1. [Long] - whole micro seconds of the time portion
     * 2. [Int] - number of days
     * 3. [Int] - number of total months
     *
     * [pg source code](https://github.com/postgres/postgres/blob/874d817baa160ca7e68bee6ccc9fc1848c56e750/src/backend/utils/adt/timestamp.c#L1032)
     */
    override fun decodeBytes(value: PgValue.Binary): PgInterval {
        val microSeconds = value.bytes.readLong()
        val days = value.bytes.readInt()
        val months = value.bytes.readInt()
        return PgInterval(months = months, days = days, microseconds = microSeconds)
    }

    /**
     * Attempt to parse the [String] into a [DateTimePeriod]. The expected format is ISO-8601 (this
     * is the interval format specified when connecting to the database).
     *
     * [pg source code](https://github.com/postgres/postgres/blob/874d817baa160ca7e68bee6ccc9fc1848c56e750/src/backend/utils/adt/timestamp.c#L983)
     */
    override fun decodeText(value: PgValue.Text): PgInterval {
        val charIter = value.text.iterator()
        var currentChar: Char
        var currentNumber = 0L
        var afterT = false
        var scale = 1
        var year = 0L
        var month = 0L
        var week = 0L
        var day = 0L
        var hour = 0L
        var minute = 0L
        var second = 0L
        var millisecond = 0L
        var microsecond = 0L
        while (charIter.hasNext()) {
            currentChar = charIter.nextChar()
            when (currentChar) {
                'P' -> {}
                'Y' -> {
                    year = currentNumber * scale
                    currentNumber = 0
                    scale = 1
                }
                'M' -> {
                    if (afterT) {
                        minute = currentNumber * scale
                        currentNumber = 0
                        scale = 1
                    } else {
                        month = currentNumber * scale
                        currentNumber = 0
                        scale = 1
                    }
                }
                'W' -> {
                    week = currentNumber * scale
                    currentNumber = 0
                    scale = 1
                }
                'D' -> {
                    day = currentNumber * scale
                    currentNumber = 0
                    scale = 1
                }
                'T' -> afterT = true
                'H' -> {
                    hour = currentNumber * scale
                    currentNumber = 0
                    scale = 1
                }
                'S' -> {
                    second = currentNumber * scale
                    currentNumber = 0
                }
                '+' -> scale = 1
                '-' -> scale = -1
                in '0'..'9' -> {
                    currentNumber = (currentNumber * 10) + (currentChar - '0')
                }
                '.' -> {
                    second = currentNumber * scale
                    currentNumber = 0
                    break
                }
                else -> error("")
            }
        }

        var secondFractionsParsed = 0
        while (charIter.hasNext()) {
            currentChar = charIter.nextChar()
            when (currentChar) {
                'S' -> {
                    val padding = secondFractionsParsed % 3
                    if (padding != 0) {
                        for (i in 1..(3 - padding)) {
                            currentNumber *= 10
                        }
                    }
                    if (secondFractionsParsed > 3) {
                        microsecond = currentNumber * scale
                    } else {
                        millisecond = currentNumber * scale
                    }
                }
                in '0'..'9' -> {
                    currentNumber = (currentNumber * 10) + (currentChar - '0')
                    if (++secondFractionsParsed == 3) {
                        millisecond = currentNumber * scale
                        currentNumber = 0
                    }
                }
            }
        }
        return PgInterval(
            months = year.toInt() * 12 + month.toInt(),
            days = week.toInt() * 7 + day.toInt(),
            microseconds =
                hour * MICROSECONDS_PER_HOUR +
                    minute * MICROSECONDS_PER_MINUTE +
                    second * MICROSECONDS_PER_SECOND +
                    millisecond * MICROSECONDS_PER_MILLISECOND +
                    microsecond,
        )
    }
}
