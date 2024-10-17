package io.github.clasicrando.kdbc.postgresql.type

import io.github.clasicrando.kdbc.core.buffer.ByteWriteBuffer
import io.github.clasicrando.kdbc.core.column.columnDecodeError
import io.github.clasicrando.kdbc.core.datetime.DateTime
import io.github.clasicrando.kdbc.core.datetime.InvalidDateString
import io.github.clasicrando.kdbc.core.datetime.tryFromString
import io.github.clasicrando.kdbc.postgresql.column.PgValue
import kotlinx.datetime.Instant
import kotlinx.datetime.UtcOffset
import kotlin.reflect.typeOf

private const val SECONDS_TO_MICROSECONDS = 1_000_000
private const val MICROSECONDS_TO_NANOSECONDS = 1_000
private const val postgresEpochSeconds = 946_684_800L
private const val postgresEpochMilliseconds = postgresEpochSeconds * 1000

private fun convertMicroSecondsOffsetToInstant(microSeconds: Long): Instant {
    var seconds = microSeconds / SECONDS_TO_MICROSECONDS
    var tempMicroSeconds = microSeconds - seconds * SECONDS_TO_MICROSECONDS
    if (tempMicroSeconds < 0) {
        seconds--
        tempMicroSeconds += SECONDS_TO_MICROSECONDS
    }
    val nanoSeconds = tempMicroSeconds * MICROSECONDS_TO_NANOSECONDS

    return Instant.fromEpochSeconds(seconds + postgresEpochSeconds, nanoSeconds)
}

/**
 * Zero instant within a postgresql database as '2000-01-01 00:00:00+00'. Datetime values (with or
 * without a timezone) sent as binary are always an offset from this [Instant].
 */
private val postgresEpochInstant = Instant.fromEpochMilliseconds(postgresEpochMilliseconds)

/**
 * Implementation of a [PgTypeDescription] for the [Instant] type. This maps to the `timestamp`
 * type in a postgresql database.
 */
internal object TimestampTypeDescription : PgTypeDescription<Instant>(
    dbType = PgType.Timestamp,
    kType = typeOf<Instant>(),
) {
    /**
     * Writes the number of microseconds since the [postgresEpochInstant] (offset shifted to UTC)
     * as a [Long] to the argument buffer.
     *
     * [pg source code](https://github.com/postgres/postgres/blob/874d817baa160ca7e68bee6ccc9fc1848c56e750/src/backend/utils/adt/timestamp.c#L259)
     */
    override fun encode(value: Instant, buffer: ByteWriteBuffer) {
        val durationSinceEpoch = value - postgresEpochInstant
        buffer.writeLong(durationSinceEpoch.inWholeMicroseconds)
    }

    /**
     * Reads a [Long] from the value and use that as the number of microseconds since the
     * [postgresEpochInstant].
     *
     * [pg source code](https://github.com/postgres/postgres/blob/874d817baa160ca7e68bee6ccc9fc1848c56e750/src/backend/utils/adt/timestamp.c#L292)
     */
    override fun decodeBytes(value: PgValue.Binary): Instant {
        val microSeconds = value.bytes.readLong()
        return convertMicroSecondsOffsetToInstant(microSeconds)
    }

    /**
     * Attempt to parse the [String] into an [Instant].
     *
     * [pg source code](https://github.com/postgres/postgres/blob/874d817baa160ca7e68bee6ccc9fc1848c56e750/src/backend/utils/adt/timestamp.c#L233)
     *
     * @throws io.github.clasicrando.kdbc.core.column.ColumnDecodeError if the text value cannot be
     * parsed into an [Instant]
     */
    override fun decodeText(value: PgValue.Text): Instant {
        return try {
            Instant.tryFromString(value.text.replace(' ', 'T'))
        } catch (ex: InvalidDateString) {
            columnDecodeError<Instant>(type = value.typeData, cause = ex)
        }
    }
}

/**
 * Implementation of a [PgTypeDescription] for the [DateTime] type. This maps to the `timestamptz`
 * type in a postgresql database.
 */
internal object TimestampTzTypeDescription : PgTypeDescription<DateTime>(
    dbType = PgType.Timestamptz,
    kType = typeOf<DateTime>(),
) {
    /**
     * Writes the number of microseconds since the [postgresEpochInstant] (offset shifted to UTC)
     * as a [Long] to the argument buffer.
     *
     * [pg source code](https://github.com/postgres/postgres/blob/874d817baa160ca7e68bee6ccc9fc1848c56e750/src/backend/utils/adt/timestamp.c#L814)
     */
    override fun encode(value: DateTime, buffer: ByteWriteBuffer) {
        val durationSinceEpoch = value.datetime - postgresEpochInstant
        buffer.writeLong(durationSinceEpoch.inWholeMicroseconds)
    }

    /**
     * Reads a [Long] from the value and use that as the number of microseconds since the
     * [postgresEpochInstant]. This value will always be at timezone UTC so the resulting decoded
     * value will also have an offset of [UtcOffset.ZERO].
     *
     * [pg source code](https://github.com/postgres/postgres/blob/874d817baa160ca7e68bee6ccc9fc1848c56e750/src/backend/utils/adt/timestamp.c#L848)
     */
    override fun decodeBytes(value: PgValue.Binary): DateTime {
        val microSeconds = value.bytes.readLong()
        val instant = convertMicroSecondsOffsetToInstant(microSeconds)
        return DateTime(datetime = instant, offset = UtcOffset.ZERO)
    }

    /**
     * Attempt to parse the [String] into a [DateTime] using the [DateTime.fromString] method.
     *
     * [pg source code](https://github.com/postgres/postgres/blob/874d817baa160ca7e68bee6ccc9fc1848c56e750/src/backend/utils/adt/timestamp.c#L786)
     *
     * @throws io.github.clasicrando.kdbc.core.column.ColumnDecodeError if the text value cannot be
     * parsed into a [Instant]
     */
    override fun decodeText(value: PgValue.Text): DateTime {
        return try {
            DateTime.fromString(value.text.replace(' ', 'T'))
        } catch (ex: InvalidDateString) {
            columnDecodeError<DateTime>(type = value.typeData, cause = ex)
        }
    }
}
