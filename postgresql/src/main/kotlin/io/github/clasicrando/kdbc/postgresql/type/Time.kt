package io.github.clasicrando.kdbc.postgresql.type

import io.github.clasicrando.kdbc.core.buffer.ByteWriteBuffer
import io.github.clasicrando.kdbc.core.column.columnDecodeError
import io.github.clasicrando.kdbc.core.datetime.InvalidDateString
import io.github.clasicrando.kdbc.core.datetime.tryFromString
import io.github.clasicrando.kdbc.postgresql.column.PgValue
import kotlinx.datetime.LocalTime
import kotlinx.datetime.UtcOffset
import java.time.OffsetTime
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter
import java.time.format.DateTimeParseException
import kotlin.reflect.typeOf
import kotlin.time.DurationUnit
import kotlin.time.toDuration

/**
 * Implementation of a [PgTypeDescription] for the [LocalTime] type. This maps to the `time` type
 * in a postgresql database.
 */
internal object LocalTimeTypeDescription : PgTypeDescription<LocalTime>(
    dbType = PgType.Time,
    kType = typeOf<LocalTime>(),
) {
    /**
     * Writes the number of microseconds since the start of the day.
     *
     * [pg source code](https://github.com/postgres/postgres/blob/874d817baa160ca7e68bee6ccc9fc1848c56e750/src/backend/utils/adt/date.c#L1521)
     */
    override fun encode(value: LocalTime, buffer: ByteWriteBuffer) {
        val microSeconds = value
            .toNanosecondOfDay()
            .toDuration(DurationUnit.NANOSECONDS)
            .inWholeMicroseconds
        buffer.writeLong(microSeconds)
    }

    /**
     * Read a [Long] value as the microseconds from the start of the data and use that to construct
     * a [LocalTime] by multiplying the [Long] value by 1000 and calling
     * [LocalTime.fromNanosecondOfDay].
     *
     * [pg source code](https://github.com/postgres/postgres/blob/874d817baa160ca7e68bee6ccc9fc1848c56e750/src/backend/utils/adt/date.c#L1547)
     */
    override fun decodeBytes(value: PgValue.Binary): LocalTime {
        val microSeconds = value.bytes.readLong()
        return LocalTime.fromNanosecondOfDay(microSeconds * 1000)
    }

    /**
     * Parse the [String] value as a [LocalTime]
     *
     * [pg source code](https://github.com/postgres/postgres/blob/874d817baa160ca7e68bee6ccc9fc1848c56e750/src/backend/utils/adt/date.c#L1501)
     *
     * @throws io.github.clasicrando.kdbc.core.column.ColumnDecodeError if the text value cannot be
     * parsed into a [LocalTime]
     */
    override fun decodeText(value: PgValue.Text): LocalTime {
        return try {
            LocalTime.tryFromString(value.text)
        } catch (ex: InvalidDateString) {
            columnDecodeError<LocalTime>(type = value.typeData, cause = ex)
        }
    }
}

/**
 * Implementation of a [PgTypeDescription] for the [PgTimeTz] type. This maps to the `timetz` type
 * in a postgresql database.
 */
internal object PgTimeTzTypeDescription : PgTypeDescription<PgTimeTz>(
    dbType = PgType.Timetz,
    kType = typeOf<PgTimeTz>(),
) {
    /**
     * Writes the number of microseconds since the start of the day followed by the number of
     * seconds offset from UTC. Since postgres treats west of UTC as positive, the
     * [UtcOffset.totalSeconds] values must be negated before writing (it treats east as positive).
     *
     * [pg source code](https://github.com/postgres/postgres/blob/874d817baa160ca7e68bee6ccc9fc1848c56e750/src/backend/utils/adt/date.c#L2335)
     */
    override fun encode(value: PgTimeTz, buffer: ByteWriteBuffer) {
        LocalTimeTypeDescription.encode(value.time, buffer)
        // Offset from postgres treats west of UTC as positive which is the opposite of UtcOffset
        buffer.writeInt(value.offset.totalSeconds * -1)
    }

    /**
     * Read a [Long] value as the microseconds from the start of the data and use that to construct
     * a [LocalTime] by multiplying the [Long] value by 1000 and calling
     * [LocalTime.fromNanosecondOfDay]. Also, read an [Int] and negate the value before
     * constructing a [UtcOffset] with that total number of seconds as the offset. The value must
     * be negated since postgres treats a positive offset as west while [UtcOffset] treats east as
     * a positive offset. Both values are then passed to the [PgTimeTz] constructor.
     *
     * [pg source code](https://github.com/postgres/postgres/blob/874d817baa160ca7e68bee6ccc9fc1848c56e750/src/backend/utils/adt/date.c#L2371)
     */
    override fun decodeBytes(value: PgValue.Binary): PgTimeTz {
        val localTime = LocalTimeTypeDescription.decodeBytes(value)
        val offsetSeconds = value.bytes.readInt() * -1
        return PgTimeTz(time = localTime, offset = UtcOffset(seconds = offsetSeconds))
    }

    /**
     * Parse the [String] value as a [PgTimeTz]
     *
     * [pg source code](https://github.com/postgres/postgres/blob/874d817baa160ca7e68bee6ccc9fc1848c56e750/src/backend/utils/adt/date.c#L2314)
     *
     * @throws io.github.clasicrando.kdbc.core.column.ColumnDecodeError if the text value cannot be
     * parsed into a [LocalTime]
     */
    override fun decodeText(value: PgValue.Text): PgTimeTz {
        return try {
            PgTimeTz.fromString(value.text)
        } catch (ex: InvalidDateString) {
            columnDecodeError<PgTimeTz>(type = value.typeData, cause = ex)
        }
    }
}

/**
 * Implementation of a [PgTypeDescription] for the [java.time.LocalTime] type. This maps to the
 * `time` type in a postgresql database.
 */
internal object JLocalTimeTypeDescription : PgTypeDescription<java.time.LocalTime>(
    dbType = PgType.Time,
    kType = typeOf<java.time.LocalTime>(),
) {
    /**
     * Writes the number of microseconds since the start of the day.
     *
     * [pg source code](https://github.com/postgres/postgres/blob/874d817baa160ca7e68bee6ccc9fc1848c56e750/src/backend/utils/adt/date.c#L1521)
     */
    override fun encode(value: java.time.LocalTime, buffer: ByteWriteBuffer) {
        val microSeconds = value
            .toNanoOfDay()
            .toDuration(DurationUnit.NANOSECONDS)
            .inWholeMicroseconds
        buffer.writeLong(microSeconds)
    }

    /**
     * Read a [Long] value as the microseconds from the start of the data and use that to construct
     * a [java.time.LocalTime] by multiplying the [Long] value by 1000 and calling
     * [java.time.LocalTime.ofNanoOfDay].
     *
     * [pg source code](https://github.com/postgres/postgres/blob/874d817baa160ca7e68bee6ccc9fc1848c56e750/src/backend/utils/adt/date.c#L1547)
     */
    override fun decodeBytes(value: PgValue.Binary): java.time.LocalTime {
        val microSeconds = value.bytes.readLong()
        return java.time.LocalTime.ofNanoOfDay(microSeconds * 1000)
    }

    /**
     * Parse the [String] value as a [java.time.LocalTime]
     *
     * [pg source code](https://github.com/postgres/postgres/blob/874d817baa160ca7e68bee6ccc9fc1848c56e750/src/backend/utils/adt/date.c#L1501)
     *
     * @throws io.github.clasicrando.kdbc.core.column.ColumnDecodeError if the text value cannot be
     * parsed into a [java.time.LocalTime]
     */
    override fun decodeText(value: PgValue.Text): java.time.LocalTime {
        return try {
            java.time.LocalTime.parse(value.text)
        } catch (ex: DateTimeParseException) {
            columnDecodeError<java.time.LocalTime>(type = value.typeData, cause = ex)
        }
    }
}

private val offsetTimeFormatter = DateTimeFormatter.ofPattern("HH:mm:ssX")

/**
 * Implementation of a [PgTypeDescription] for the [OffsetTime] type. This maps to the `timetz` type
 * in a postgresql database.
 */
internal object OffsetTimeTypeDescription : PgTypeDescription<OffsetTime>(
    dbType = PgType.Timetz,
    kType = typeOf<OffsetTime>(),
) {
    /**
     * Writes the number of microseconds since the start of the day followed by the number of
     * seconds offset from UTC. Since postgres treats west of UTC as positive, the
     * [UtcOffset.totalSeconds] values must be negated before writing (it treats east as positive).
     *
     * [pg source code](https://github.com/postgres/postgres/blob/874d817baa160ca7e68bee6ccc9fc1848c56e750/src/backend/utils/adt/date.c#L2335)
     */
    override fun encode(value: OffsetTime, buffer: ByteWriteBuffer) {
        JLocalTimeTypeDescription.encode(value.toLocalTime(), buffer)
        // Offset from postgres treats west of UTC as positive which is the opposite of UtcOffset
        buffer.writeInt(value.offset.totalSeconds * -1)
    }

    /**
     * Read a [Long] value as the microseconds from the start of the data and use that to construct
     * a [LocalTime] by multiplying the [Long] value by 1000 and calling
     * [LocalTime.fromNanosecondOfDay]. Also, read an [Int] and negate the value before
     * constructing a [UtcOffset] with that total number of seconds as the offset. The value must
     * be negated since postgres treats a positive offset as west while [UtcOffset] treats east as
     * a positive offset. Both values are then passed to the [PgTimeTz] constructor.
     *
     * [pg source code](https://github.com/postgres/postgres/blob/874d817baa160ca7e68bee6ccc9fc1848c56e750/src/backend/utils/adt/date.c#L2371)
     */
    override fun decodeBytes(value: PgValue.Binary): OffsetTime {
        val localTime = JLocalTimeTypeDescription.decodeBytes(value)
        val offsetSeconds = value.bytes.readInt() * -1
        return OffsetTime.of(localTime, ZoneOffset.ofTotalSeconds(offsetSeconds))
    }

    /**
     * Parse the [String] value as a [PgTimeTz]
     *
     * [pg source code](https://github.com/postgres/postgres/blob/874d817baa160ca7e68bee6ccc9fc1848c56e750/src/backend/utils/adt/date.c#L2314)
     *
     * @throws io.github.clasicrando.kdbc.core.column.ColumnDecodeError if the text value cannot be
     * parsed into a [LocalTime]
     */
    override fun decodeText(value: PgValue.Text): OffsetTime {
        return try {
            OffsetTime.parse(value.text, offsetTimeFormatter)
        } catch (ex: DateTimeParseException) {
            columnDecodeError<OffsetTime>(type = value.typeData, cause = ex)
        }
    }
}
