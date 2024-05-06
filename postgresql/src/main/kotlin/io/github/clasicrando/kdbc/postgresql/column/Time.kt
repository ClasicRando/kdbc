package io.github.clasicrando.kdbc.postgresql.column

import io.github.clasicrando.kdbc.core.buffer.ByteWriteBuffer
import io.github.clasicrando.kdbc.core.column.ColumnDecodeError
import io.github.clasicrando.kdbc.core.column.columnDecodeError
import io.github.clasicrando.kdbc.core.datetime.InvalidDateString
import io.github.clasicrando.kdbc.core.datetime.tryFromString
import io.github.clasicrando.kdbc.postgresql.type.PgTimeTz
import kotlinx.datetime.LocalTime
import kotlinx.datetime.UtcOffset
import kotlin.reflect.typeOf
import kotlin.time.DurationUnit
import kotlin.time.toDuration

/**
 * Implementation of a [PgTypeDescription] for the [LocalTime] type. This maps to the `time` type
 * in a postgresql database.
 */
object TimeTypeDescription : PgTypeDescription<LocalTime>(
    pgType = PgType.Time,
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
     * @throws ColumnDecodeError if the text value cannot be parsed into a [LocalTime]
     */
    override fun decodeText(value: PgValue.Text): LocalTime {
        return try {
            LocalTime.tryFromString(value.text)
        } catch (ex: InvalidDateString) {
            columnDecodeError<LocalTime>(type = value.typeData, reason = ex.message ?: "")
        }
    }
}

/**
 * Implementation of a [ArrayTypeDescription] for [LocalTime]. This maps to the `time[]` type in a
 * postgresql database.
 */
object TimeArrayTypeDescription : ArrayTypeDescription<LocalTime>(
    pgType = PgType.TimeArray,
    innerType = TimeTypeDescription,
)

/**
 * Implementation of a [PgTypeDescription] for the [PgTimeTz] type. This maps to the `timetz` type
 * in a postgresql database.
 */
object TimeTzTypeDescription : PgTypeDescription<PgTimeTz>(
    pgType = PgType.Timetz,
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
        TimeTypeDescription.encode(value.time, buffer)
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
        val localTime = TimeTypeDescription.decodeBytes(value)
        val offsetSeconds = value.bytes.readInt() * -1
        return PgTimeTz(time = localTime, offset = UtcOffset(seconds = offsetSeconds))
    }

    /**
     * Parse the [String] value as a [PgTimeTz]
     *
     * [pg source code](https://github.com/postgres/postgres/blob/874d817baa160ca7e68bee6ccc9fc1848c56e750/src/backend/utils/adt/date.c#L2314)
     *
     * @throws ColumnDecodeError if the text value cannot be parsed into a [LocalTime]
     */
    override fun decodeText(value: PgValue.Text): PgTimeTz {
        return try {
            PgTimeTz.fromString(value.text)
        } catch (ex: InvalidDateString) {
            columnDecodeError<PgTimeTz>(type = value.typeData, reason = ex.message ?: "")
        }
    }
}

/**
 * Implementation of a [ArrayTypeDescription] for [PgTimeTz]. This maps to the `timetz[]` type in a
 * postgresql database.
 */
object TimeTzArrayTypeDescription : ArrayTypeDescription<PgTimeTz>(
    pgType = PgType.TimetzArray,
    innerType = TimeTzTypeDescription,
)
