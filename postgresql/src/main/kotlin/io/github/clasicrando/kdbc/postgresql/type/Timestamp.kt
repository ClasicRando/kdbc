package io.github.clasicrando.kdbc.postgresql.type

import io.github.clasicrando.kdbc.core.column.columnDecodeError
import io.github.clasicrando.kdbc.postgresql.column.PgValue
import java.time.Instant
import java.time.LocalDateTime
import java.time.OffsetDateTime
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter
import java.time.format.DateTimeParseException
import java.time.temporal.ChronoUnit
import kotlin.reflect.typeOf
import kotlinx.io.Sink

private const val SECONDS_TO_MICROSECONDS = 1_000_000
private const val MICROSECONDS_TO_NANOSECONDS = 1_000
private const val POSTGRES_EPOCH_SECONDS = 946_684_800L
private const val POSTGRES_EPOCH_MILLISECONDS = POSTGRES_EPOCH_SECONDS * 1000

private fun convertMicroSecondsOffsetToLocalDateTime(microSeconds: Long): LocalDateTime {
    var seconds = microSeconds / SECONDS_TO_MICROSECONDS
    var tempMicroSeconds = microSeconds - seconds * SECONDS_TO_MICROSECONDS
    if (tempMicroSeconds < 0) {
        seconds--
        tempMicroSeconds += SECONDS_TO_MICROSECONDS
    }
    val nanoSeconds = tempMicroSeconds * MICROSECONDS_TO_NANOSECONDS

    return LocalDateTime.ofEpochSecond(
        seconds + POSTGRES_EPOCH_SECONDS,
        nanoSeconds.toInt(),
        ZoneOffset.UTC,
    )
}

/**
 * Zero instant within a postgresql database as '2000-01-01 00:00:00+00'. Datetime values (with or
 * without a timezone) sent as binary are always an offset from this [Instant].
 */
private val postgresEpochInstant = Instant.ofEpochMilli(POSTGRES_EPOCH_MILLISECONDS)
private val postgresEpochLocalDateTime =
    LocalDateTime.ofInstant(postgresEpochInstant, ZoneOffset.UTC)

/**
 * Implementation of a [PgTypeDescription] for the [LocalDateTime] type. This maps to the
 * `timestamp` type in a postgresql database.
 */
internal object LocalDateTimeTypeDescription :
    PgTypeDescription<LocalDateTime>(dbType = PgType.Timestamp, kType = typeOf<LocalDateTime>()) {
    private val formatter = DateTimeFormatter.ofPattern("uuuu-MM-dd HH:mm:ss[.S][X]")

    override fun isCompatible(dbType: PgType): Boolean {
        return dbType == PgType.Timestamp || dbType == PgType.Timestamptz
    }

    /**
     * Writes the number of microseconds since the [postgresEpochInstant] (offset shifted to UTC) as
     * a [Long] to the argument buffer.
     *
     * [code](https://github.com/postgres/postgres/blob/874d817baa160ca7e68bee6ccc9fc1848c56e750/src/backend/utils/adt/timestamp.c#L259)
     */
    override fun encode(value: LocalDateTime, buffer: Sink) {
        val durationSinceEpoch = postgresEpochLocalDateTime.until(value, ChronoUnit.MICROS)
        buffer.writeLong(durationSinceEpoch)
    }

    /**
     * Reads a [Long] from the value and use that as the number of microseconds since the
     * [postgresEpochInstant].
     *
     * [code](https://github.com/postgres/postgres/blob/874d817baa160ca7e68bee6ccc9fc1848c56e750/src/backend/utils/adt/timestamp.c#L292)
     */
    override fun decodeBytes(value: PgValue.Binary): LocalDateTime {
        val microSeconds = value.bytes.readLong()
        return convertMicroSecondsOffsetToLocalDateTime(microSeconds)
    }

    /**
     * Attempt to parse the [String] into an [Instant].
     *
     * [code](https://github.com/postgres/postgres/blob/874d817baa160ca7e68bee6ccc9fc1848c56e750/src/backend/utils/adt/timestamp.c#L233)
     *
     * @throws io.github.clasicrando.kdbc.core.column.ColumnDecodeError if the text value cannot be
     *   parsed into an [Instant]
     */
    override fun decodeText(value: PgValue.Text): LocalDateTime {
        return try {
            LocalDateTime.parse(value.text, formatter)
        } catch (ex: DateTimeParseException) {
            columnDecodeError<LocalDateTime>(type = value.typeData, cause = ex)
        }
    }
}

/**
 * Implementation of a [PgTypeDescription] for the [Instant] type. This maps to the `timestamp` type
 * in a postgresql database.
 */
internal class InstantTypeDescription(private val zoneOffset: ZoneOffset) :
    PgTypeDescription<Instant>(dbType = PgType.Timestamp, kType = typeOf<Instant>()) {
    override fun isCompatible(dbType: PgType): Boolean {
        return LocalDateTimeTypeDescription.isCompatible(dbType)
    }

    /**
     * Writes the number of microseconds since the [postgresEpochInstant] (offset shifted to UTC) as
     * a [Long] to the argument buffer.
     *
     * [code](https://github.com/postgres/postgres/blob/874d817baa160ca7e68bee6ccc9fc1848c56e750/src/backend/utils/adt/timestamp.c#L259)
     */
    override fun encode(value: Instant, buffer: Sink) {
        LocalDateTimeTypeDescription.encode(
            value = LocalDateTime.ofInstant(value, ZoneOffset.UTC),
            buffer = buffer,
        )
    }

    /**
     * Reads a [Long] from the value and use that as the number of microseconds since the
     * [postgresEpochInstant].
     *
     * [code](https://github.com/postgres/postgres/blob/874d817baa160ca7e68bee6ccc9fc1848c56e750/src/backend/utils/adt/timestamp.c#L292)
     */
    override fun decodeBytes(value: PgValue.Binary): Instant {
        return LocalDateTimeTypeDescription.decodeBytes(value).toInstant(zoneOffset)
    }

    /**
     * Attempt to parse the [String] into an [Instant].
     *
     * [code](https://github.com/postgres/postgres/blob/874d817baa160ca7e68bee6ccc9fc1848c56e750/src/backend/utils/adt/timestamp.c#L233)
     *
     * @throws io.github.clasicrando.kdbc.core.column.ColumnDecodeError if the text value cannot be
     *   parsed into an [Instant]
     */
    override fun decodeText(value: PgValue.Text): Instant {
        val localDateTime = LocalDateTimeTypeDescription.decodeText(value)
        return localDateTime.toInstant(zoneOffset)
    }
}

/**
 * Implementation of a [PgTypeDescription] for the [OffsetDateTime] type. This maps to the
 * `timestamptz` type in a postgresql database.
 */
internal class OffsetDateTimeTypeDescription(private val zoneOffset: ZoneOffset) :
    PgTypeDescription<OffsetDateTime>(
        dbType = PgType.Timestamptz,
        kType = typeOf<OffsetDateTime>(),
    ) {
    override fun isCompatible(dbType: PgType): Boolean {
        return LocalDateTimeTypeDescription.isCompatible(dbType)
    }

    /**
     * Writes the number of microseconds since the [postgresEpochInstant] (offset shifted to UTC) as
     * a [Long] to the argument buffer.
     *
     * [code](https://github.com/postgres/postgres/blob/874d817baa160ca7e68bee6ccc9fc1848c56e750/src/backend/utils/adt/timestamp.c#L814)
     */
    override fun encode(value: OffsetDateTime, buffer: Sink) {
        val localDateTime = value.atZoneSameInstant(ZoneOffset.UTC).toLocalDateTime()
        LocalDateTimeTypeDescription.encode(localDateTime, buffer)
    }

    /**
     * Reads a [Long] from the value and use that as the number of microseconds since the
     * [postgresEpochInstant]. This value will always be at timezone UTC so the resulting decoded
     * value will also have an offset of [ZoneOffset.UTC].
     *
     * [code](https://github.com/postgres/postgres/blob/874d817baa160ca7e68bee6ccc9fc1848c56e750/src/backend/utils/adt/timestamp.c#L848)
     */
    override fun decodeBytes(value: PgValue.Binary): OffsetDateTime {
        val dateTime = LocalDateTimeTypeDescription.decodeBytes(value)
        return OffsetDateTime.of(dateTime, ZoneOffset.UTC)
    }

    /**
     * Attempt to parse the [String] into an [OffsetDateTime] using the [OffsetDateTime.parse]
     * method.
     *
     * [code](https://github.com/postgres/postgres/blob/874d817baa160ca7e68bee6ccc9fc1848c56e750/src/backend/utils/adt/timestamp.c#L786)
     *
     * @throws io.github.clasicrando.kdbc.core.column.ColumnDecodeError if the text value cannot be
     *   parsed into a [Instant]
     */
    override fun decodeText(value: PgValue.Text): OffsetDateTime {
        val localDateTime = LocalDateTimeTypeDescription.decodeText(value)
        return localDateTime.atOffset(zoneOffset)
    }
}
