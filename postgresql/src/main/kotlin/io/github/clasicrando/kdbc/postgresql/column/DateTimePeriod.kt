package io.github.clasicrando.kdbc.postgresql.column

import io.github.clasicrando.kdbc.core.buffer.ByteWriteBuffer
import kotlinx.datetime.DateTimePeriod
import kotlin.reflect.typeOf

private const val minutesPerHour = 60L
private const val secondsPerMinute = 60L
private const val microSecondsPerSecond = 1_000_000L
private const val nanoSecondsPerMicroSeconds = 1_000.0
private const val microSecondsPerHour = minutesPerHour * secondsPerMinute * microSecondsPerSecond
private const val microSecondsPerMinute = secondsPerMinute * microSecondsPerSecond

/**
 * Extract the number of whole microseconds from this [DateTimePeriod] rounding down for fractional
 * nanoseconds present.
 */
private fun DateTimePeriod.inWholeMicroSeconds(): Long {
    return this.hours * microSecondsPerHour +
            this.minutes * microSecondsPerMinute +
            this.seconds * microSecondsPerSecond +
            kotlin.math.floor(this.nanoseconds / nanoSecondsPerMicroSeconds).toLong()
}

object IntervalTypeDescription : PgTypeDescription<DateTimePeriod>(
    pgType = PgType.Interval,
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
    override fun encode(value: DateTimePeriod, buffer: ByteWriteBuffer) {
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
    override fun decodeText(value: PgValue.Text): DateTimePeriod {
        return DateTimePeriod.parse(value.text)
    }
}

/**
 * Implementation of a [ArrayTypeDescription] for [DateTimePeriod]. This maps to the `interval[]`
 * type in a postgresql database.
 */
object IntervalArrayTypeDescription : ArrayTypeDescription<DateTimePeriod>(
    pgType = PgType.IntervalArray,
    innerType = IntervalTypeDescription,
)
