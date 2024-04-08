package io.github.clasicrando.kdbc.postgresql.column

import io.github.clasicrando.kdbc.core.column.ColumnDecodeError
import io.github.clasicrando.kdbc.core.column.columnDecodeError
import io.github.clasicrando.kdbc.core.datetime.InvalidDateString
import io.github.clasicrando.kdbc.core.datetime.tryFromString
import io.github.clasicrando.kdbc.postgresql.type.PgTimeTz
import kotlinx.datetime.LocalTime
import kotlinx.datetime.UtcOffset
import kotlin.time.DurationUnit
import kotlin.time.toDuration

/**
 * Implementation of [PgTypeEncoder] for [LocalTime]. This maps to the `time without timezone` type
 * in a postgresql database. Writes the number of microseconds since the start of the day.
 *
 * [pg source code](https://github.com/postgres/postgres/blob/874d817baa160ca7e68bee6ccc9fc1848c56e750/src/backend/utils/adt/date.c#L1521)
 */
val timeTypeEncoder = PgTypeEncoder<LocalTime>(PgType.Time) { value, buffer ->
    val microSeconds = value
        .toNanosecondOfDay()
        .toDuration(DurationUnit.NANOSECONDS)
        .inWholeMicroseconds
    buffer.writeLong(microSeconds)
}

/**
 * Implementation of [PgTypeDecoder] for [LocalTime]. This maps to the `time without timezone` type
 * in a postgresql database.
 *
 * ### Binary
 * Read a [Long] value as the microseconds from the start of the data and use that to construct a
 * [LocalTime] by multiplying the [Long] value by 1000 and calling [LocalTime.fromNanosecondOfDay].
 *
 * ### Text
 * Parse the [String] value as a [LocalTime]
 *
 * [pg source code binary](https://github.com/postgres/postgres/blob/874d817baa160ca7e68bee6ccc9fc1848c56e750/src/backend/utils/adt/date.c#L1547)
 * [pg source code text](https://github.com/postgres/postgres/blob/874d817baa160ca7e68bee6ccc9fc1848c56e750/src/backend/utils/adt/date.c#L1501)
 *
 * @throws ColumnDecodeError if the text value cannot be parsed into a [LocalTime]
 */
val timeTypeDecoder = PgTypeDecoder { value ->
    when (value) {
        is PgValue.Binary -> {
            val microSeconds = value.bytes.readLong()
            LocalTime.fromNanosecondOfDay(microSeconds * 1000)
        }
        is PgValue.Text -> try {
            LocalTime.tryFromString(value.text)
        } catch (ex: InvalidDateString) {
            columnDecodeError<LocalTime>(type = value.typeData, reason = ex.message ?: "")
        }
    }
}

/**
 * Implementation of [PgTypeEncoder] for [LocalTime]. This maps to the `time with timezone` type in
 * a postgresql database. Writes the number of microseconds since the start of the day followed by
 * the number of seconds offset from UTC. Since postgres treats west of UTC as positive, the
 * [UtcOffset.totalSeconds] values must be negated before writing (it treats east as positive).
 *
 * [pg source code](https://github.com/postgres/postgres/blob/874d817baa160ca7e68bee6ccc9fc1848c56e750/src/backend/utils/adt/date.c#L2335)
 */
val timeTzTypeEncoder = PgTypeEncoder<PgTimeTz>(PgType.Timetz) { value, buffer ->
    timeTypeEncoder.encode(value.time, buffer)
    // Offset from postgres treats west of UTC as positive which is the opposite of UtcOffset
    buffer.writeInt(value.offset.totalSeconds * -1)
}

/**
 * Implementation of [PgTypeDecoder] for [PgTimeTz]. This maps to the `time with timezone` type in
 * a postgresql database.
 *
 * ### Binary
 * Read a [Long] value as the microseconds from the start of the data and use that to construct a
 * [LocalTime] by multiplying the [Long] value by 1000 and calling [LocalTime.fromNanosecondOfDay].
 * Also, read an [Int] and negate the value before constructing a [UtcOffset] with that total
 * number of seconds as the offset. The value must be negated since postgres treats a positive
 * offset as west while [UtcOffset] treats east as a positive offset. Both values are then passed
 * to the [PgTimeTz] constructor.
 *
 * ### Text
 * Parse the [String] value as a [PgTimeTz]
 *
 * [pg source code binary](https://github.com/postgres/postgres/blob/874d817baa160ca7e68bee6ccc9fc1848c56e750/src/backend/utils/adt/date.c#L2371)
 * [pg source code text](https://github.com/postgres/postgres/blob/874d817baa160ca7e68bee6ccc9fc1848c56e750/src/backend/utils/adt/date.c#L2314)
 *
 * @throws ColumnDecodeError if the text value cannot be parsed into a [LocalTime]
 */
val timeTzTypeDecoder = PgTypeDecoder { value ->
    when (value) {
        is PgValue.Binary -> {
            val microSeconds = value.bytes.readLong()
            val offsetSeconds = value.bytes.readInt() * -1
            PgTimeTz(
                time = LocalTime.fromNanosecondOfDay(microSeconds * 1000),
                offset = UtcOffset(seconds = offsetSeconds),
            )
        }
        is PgValue.Text -> try {
            PgTimeTz.fromString(value.text)
        } catch (ex: InvalidDateString) {
            columnDecodeError<PgTimeTz>(type = value.typeData, reason = ex.message ?: "")
        }
    }
}
