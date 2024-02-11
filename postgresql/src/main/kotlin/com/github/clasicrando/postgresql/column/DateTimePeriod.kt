package com.github.clasicrando.postgresql.column

import io.ktor.utils.io.core.writeInt
import io.ktor.utils.io.core.writeLong
import kotlinx.datetime.DateTimePeriod

private const val minutesPerHour = 60L
private const val secondsPerMinute = 60L
private const val microSecondsPerSecond = 1_000_000L
private const val nanoSecondsPerMicroSeconds = 1_000L

private fun DateTimePeriod.inWholeMicroSeconds(): Long {
    return this.hours * minutesPerHour * secondsPerMinute * microSecondsPerSecond +
            this.minutes * secondsPerMinute * microSecondsPerSecond +
            this.seconds * microSecondsPerSecond +
            this.nanoseconds / nanoSecondsPerMicroSeconds
}

val dateTimePeriodTypeEncoder = PgTypeEncoder<DateTimePeriod>(PgType.Interval) { value, buffer ->
    buffer.writeLong(value.inWholeMicroSeconds())
    buffer.writeInt(value.days)
    buffer.writeInt(value.years * 12 + value.months)
}

val dateTimePeriodTypeDecoder = PgTypeDecoder { value ->
    when (value) {
        is PgValue.Binary -> {
            val microSeconds = value.bytes.readLong()
            val days = value.bytes.readInt()
            val months = value.bytes.readInt()
            DateTimePeriod(months = months, days = days, nanoseconds = microSeconds * 1000)
        }
        is PgValue.Text -> DateTimePeriod.parse(value.text)
    }
}
