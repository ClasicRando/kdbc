package io.github.clasicrando.kdbc.postgresql.type

import io.github.clasicrando.kdbc.core.buffer.ByteWriteBuffer
import io.github.clasicrando.kdbc.core.column.columnDecodeError
import io.github.clasicrando.kdbc.postgresql.column.PgValue
import java.time.LocalTime
import java.time.OffsetTime
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter
import java.time.format.DateTimeParseException
import kotlin.reflect.typeOf
import kotlin.time.DurationUnit
import kotlin.time.toDuration

/**
 * Implementation of a [PgTypeDescription] for the [LocalTime] type. This maps to the `time` type in
 * a postgresql database.
 */
internal object LocalTimeTypeDescription :
    PgTypeDescription<LocalTime>(dbType = PgType.Time, kType = typeOf<LocalTime>()) {
    /**
     * Writes the number of microseconds since the start of the day.
     *
     * [code](https://github.com/postgres/postgres/blob/874d817baa160ca7e68bee6ccc9fc1848c56e750/src/backend/utils/adt/date.c#L1521)
     */
    override fun encode(value: LocalTime, buffer: ByteWriteBuffer) {
        val microSeconds =
            value.toNanoOfDay().toDuration(DurationUnit.NANOSECONDS).inWholeMicroseconds
        buffer.writeLong(microSeconds)
    }

    /**
     * Read a [Long] value as the microseconds from the start of the data and use that to construct
     * a [LocalTime] by multiplying the [Long] value by 1000 and calling [LocalTime.ofNanoOfDay].
     *
     * [code](https://github.com/postgres/postgres/blob/874d817baa160ca7e68bee6ccc9fc1848c56e750/src/backend/utils/adt/date.c#L1547)
     */
    override fun decodeBytes(value: PgValue.Binary): LocalTime {
        val microSeconds = value.bytes.readLong()
        return LocalTime.ofNanoOfDay(microSeconds * 1000)
    }

    /**
     * Parse the [String] value as a [LocalTime]
     *
     * [code](https://github.com/postgres/postgres/blob/874d817baa160ca7e68bee6ccc9fc1848c56e750/src/backend/utils/adt/date.c#L1501)
     *
     * @throws io.github.clasicrando.kdbc.core.column.ColumnDecodeError if the text value cannot be
     *   parsed into a [LocalTime]
     */
    override fun decodeText(value: PgValue.Text): LocalTime {
        return try {
            LocalTime.parse(value.text)
        } catch (ex: DateTimeParseException) {
            columnDecodeError<LocalTime>(type = value.typeData, cause = ex)
        }
    }
}

private val offsetTimeFormatter = DateTimeFormatter.ofPattern("HH:mm:ssX")

/**
 * Implementation of a [PgTypeDescription] for the [OffsetTime] type. This maps to the `timetz` type
 * in a postgresql database.
 */
internal object OffsetTimeTypeDescription :
    PgTypeDescription<OffsetTime>(dbType = PgType.Timetz, kType = typeOf<OffsetTime>()) {
    /**
     * Writes the number of microseconds since the start of the day followed by the number of
     * seconds offset from UTC. Since postgres treats west of UTC as positive, the
     * [ZoneOffset.totalSeconds] values must be negated before writing (it treats east as positive).
     *
     * [code](https://github.com/postgres/postgres/blob/874d817baa160ca7e68bee6ccc9fc1848c56e750/src/backend/utils/adt/date.c#L2335)
     */
    override fun encode(value: OffsetTime, buffer: ByteWriteBuffer) {
        LocalTimeTypeDescription.encode(value.toLocalTime(), buffer)
        // Offset from postgres treats west of UTC as positive which is the opposite of UtcOffset
        buffer.writeInt(value.offset.totalSeconds * -1)
    }

    /**
     * The bytes of this type are the same as [LocalTime] with a final [Int] describing the offset
     * from UTC as it is stored.
     *
     * [code](https://github.com/postgres/postgres/blob/874d817baa160ca7e68bee6ccc9fc1848c56e750/src/backend/utils/adt/date.c#L2371)
     */
    override fun decodeBytes(value: PgValue.Binary): OffsetTime {
        val localTime = LocalTimeTypeDescription.decodeBytes(value)
        val offsetSeconds = value.bytes.readInt() * -1
        return OffsetTime.of(localTime, ZoneOffset.ofTotalSeconds(offsetSeconds))
    }

    /**
     * Parse the [String] value as a [OffsetTime]
     *
     * [code](https://github.com/postgres/postgres/blob/874d817baa160ca7e68bee6ccc9fc1848c56e750/src/backend/utils/adt/date.c#L2314)
     *
     * @throws io.github.clasicrando.kdbc.core.column.ColumnDecodeError if the text value cannot be
     *   parsed into a [LocalTime]
     */
    override fun decodeText(value: PgValue.Text): OffsetTime =
        try {
            OffsetTime.parse(value.text, offsetTimeFormatter)
        } catch (ex: DateTimeParseException) {
            columnDecodeError<OffsetTime>(type = value.typeData, cause = ex)
        }
}
