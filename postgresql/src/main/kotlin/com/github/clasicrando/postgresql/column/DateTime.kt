package com.github.clasicrando.postgresql.column

import com.github.clasicrando.common.column.ColumnDecodeError
import com.github.clasicrando.common.column.columnDecodeError
import com.github.clasicrando.common.datetime.DateTime
import com.github.clasicrando.common.datetime.InvalidDateString
import com.github.clasicrando.common.datetime.tryFromString
import kotlinx.datetime.LocalDateTime
import kotlinx.datetime.TimeZone
import kotlinx.datetime.UtcOffset
import kotlinx.datetime.toInstant
import kotlinx.datetime.toLocalDateTime
import kotlin.time.DurationUnit
import kotlin.time.toDuration

/**
 * Zero instant within a postgresql database. Datetime values (with or without a timezone) sent as
 * binary are always an offset from this [Instant][kotlinx.datetime.Instant]
 */
private val postgresEpochInstant = LocalDateTime(
    year = 2000,
    monthNumber = 1,
    dayOfMonth = 1,
    hour = 0,
    minute = 0,
    second = 0,
).toInstant(UtcOffset.ZERO)

/**
 * Implementation of a [PgTypeEncoder] for the [DateTime] type. This maps to the `timestamp with
 * timezone` type in a postgresql database. The encoder writes the number of microseconds since the
 * [postgresEpochInstant] (offset shifted to UTC) as a [Long] to the argument buffer.
 *
 * [pg source code](https://github.com/postgres/postgres/blob/874d817baa160ca7e68bee6ccc9fc1848c56e750/src/backend/utils/adt/timestamp.c#L814)
 */
val dateTimeTypeEncoder = PgTypeEncoder<DateTime>(PgType.Timestamptz) { value, buffer ->
    val durationSinceEpoch = value.datetime.toInstant(value.offset) - postgresEpochInstant
    buffer.writeLong(durationSinceEpoch.inWholeMicroseconds)
}

/**
 * Implementation of a [PgTypeDecoder] for the [DateTime] type. This maps to the `timestamp with
 * timezone` type in a postgresql database.
 *
 * ### Binary
 * Reads a [Long] from the value and use that as the number of microseconds since the
 * [postgresEpochInstant]. This value will always be at timezone UTC so the resulting decoded value
 * will also have an offset of [UtcOffset.ZERO].
 *
 * ### Text
 * Attempt to parse the [String] into a [DateTime] using the [DateTime.fromString] method.
 *
 * [pg source code binary](https://github.com/postgres/postgres/blob/874d817baa160ca7e68bee6ccc9fc1848c56e750/src/backend/utils/adt/timestamp.c#L848)
 * [pg source code text](https://github.com/postgres/postgres/blob/874d817baa160ca7e68bee6ccc9fc1848c56e750/src/backend/utils/adt/timestamp.c#L786)
 *
 * @throws ColumnDecodeError if the text value cannot be parsed into a [DateTime]
 */
val dateTimeTypeDecoder = PgTypeDecoder { value ->
    when (value) {
        is PgValue.Binary -> {
            val microSeconds = value.bytes.readLong()
            val instant = postgresEpochInstant + microSeconds.toDuration(DurationUnit.MICROSECONDS)
            DateTime(datetime = instant.toLocalDateTime(TimeZone.UTC), offset = UtcOffset.ZERO)
        }
        is PgValue.Text -> try {
            DateTime.fromString(value.text)
        } catch (ex: InvalidDateString) {
            columnDecodeError<DateTime>(type = value.typeData, reason = ex.message ?: "")
        }
    }
}

/**
 * Implementation of a [PgTypeEncoder] for the [DateTime] type. This maps to the `timestamp with
 * timezone` type in a postgresql database. The encoder writes the number of microseconds since the
 * [postgresEpochInstant] (offset shifted to UTC) as a [Long] to the argument buffer.
 *
 * [pg source code](https://github.com/postgres/postgres/blob/874d817baa160ca7e68bee6ccc9fc1848c56e750/src/backend/utils/adt/timestamp.c#L814)
 */
// https://github.com/postgres/postgres/blob/874d817baa160ca7e68bee6ccc9fc1848c56e750/src/backend/utils/adt/timestamp.c#L259
val localDateTimeTypeEncoder = PgTypeEncoder<LocalDateTime>(PgType.Timestamp) { value, buffer ->
    val durationSinceEpoch = value.toInstant(UtcOffset.ZERO) - postgresEpochInstant
    buffer.writeLong(durationSinceEpoch.inWholeMicroseconds)
}

/**
 * Implementation of a [PgTypeDecoder] for the [LocalDateTime] type. This maps to the `timestamp
 * without timezone` type in a postgresql database.
 *
 * ### Binary
 * Reads a [Long] from the value and use that as the number of microseconds since the
 * [postgresEpochInstant].
 *
 * ### Text
 * Attempt to parse the [String] into a [LocalDateTime].
 *
 * [pg source code binary](https://github.com/postgres/postgres/blob/874d817baa160ca7e68bee6ccc9fc1848c56e750/src/backend/utils/adt/timestamp.c#L292)
 * [pg source code text](https://github.com/postgres/postgres/blob/874d817baa160ca7e68bee6ccc9fc1848c56e750/src/backend/utils/adt/timestamp.c#L233)
 *
 * @throws ColumnDecodeError if the text value cannot be parsed into a [LocalDateTime]
 */
val localDateTimeTypeDecoder = PgTypeDecoder { value ->
    when (value) {
        is PgValue.Binary -> {
            val microSeconds = value.bytes.readLong()
            val instant = postgresEpochInstant + microSeconds.toDuration(DurationUnit.MICROSECONDS)
            instant.toLocalDateTime(TimeZone.UTC)
        }
        is PgValue.Text -> try {
            LocalDateTime.tryFromString(value.text)
        } catch (ex: InvalidDateString) {
            columnDecodeError<LocalDateTime>(type = value.typeData, reason = ex.message ?: "")
        }
    }
}
