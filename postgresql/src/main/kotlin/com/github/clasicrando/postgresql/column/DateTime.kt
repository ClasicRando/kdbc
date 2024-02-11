package com.github.clasicrando.postgresql.column

import com.github.clasicrando.common.datetime.DateTime
import com.github.clasicrando.common.datetime.tryFromString
import kotlinx.datetime.LocalDateTime
import kotlinx.datetime.TimeZone
import kotlinx.datetime.UtcOffset
import kotlinx.datetime.toInstant
import kotlinx.datetime.toLocalDateTime
import kotlin.time.DurationUnit
import kotlin.time.toDuration

private val postgresEpochInstant = LocalDateTime(
    year = 2000,
    monthNumber = 1,
    dayOfMonth = 1,
    hour = 0,
    minute = 0,
    second = 0,
).toInstant(UtcOffset.ZERO)

val dateTimeTypeEncoder = PgTypeEncoder<DateTime>(PgType.Timestamptz) { value, buffer ->
    val durationSinceEpoch = value.datetime.toInstant(value.offset) - postgresEpochInstant
    buffer.writeLong(durationSinceEpoch.inWholeMicroseconds)
}

val dateTimeTypeDecoder = PgTypeDecoder { value ->
    when (value) {
        is PgValue.Binary -> {
            val microSeconds = value.bytes.readLong()
            val instant = postgresEpochInstant + microSeconds.toDuration(DurationUnit.MICROSECONDS)
            DateTime(datetime = instant.toLocalDateTime(TimeZone.UTC), offset = UtcOffset.ZERO)
        }
        is PgValue.Text -> DateTime.fromString(value.text)
    }
}

val localDateTimeTypeEncoder = PgTypeEncoder<LocalDateTime>(PgType.Timestamp) { value, buffer ->
    val durationSinceEpoch = value.toInstant(UtcOffset.ZERO) - postgresEpochInstant
    buffer.writeLong(durationSinceEpoch.inWholeMicroseconds)
}

val localDateTimeTypeDecoder = PgTypeDecoder { value ->
    when (value) {
        is PgValue.Binary -> {
            val microSeconds = value.bytes.readLong()
            val instant = postgresEpochInstant + microSeconds.toDuration(DurationUnit.MICROSECONDS)
            instant.toLocalDateTime(TimeZone.UTC)
        }
        is PgValue.Text -> LocalDateTime.tryFromString(value.text).getOrThrow()
    }
}
