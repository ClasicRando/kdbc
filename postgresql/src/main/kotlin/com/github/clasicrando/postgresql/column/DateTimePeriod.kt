package com.github.clasicrando.postgresql.column

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

// https://github.com/postgres/postgres/blob/874d817baa160ca7e68bee6ccc9fc1848c56e750/src/backend/utils/adt/timestamp.c#L1007
val dateTimePeriodTypeEncoder = PgTypeEncoder<DateTimePeriod>(PgType.Interval) { value, buffer ->
    buffer.writeLong(value.inWholeMicroSeconds())
    buffer.writeInt(value.days)
    buffer.writeInt(value.years * 12 + value.months)
}

val dateTimePeriodTypeDecoder = PgTypeDecoder { value ->
    when (value) {
        // https://github.com/postgres/postgres/blob/874d817baa160ca7e68bee6ccc9fc1848c56e750/src/backend/utils/adt/timestamp.c#L1032
        is PgValue.Binary -> {
            val microSeconds = value.bytes.readLong()
            val days = value.bytes.readInt()
            val months = value.bytes.readInt()
            DateTimePeriod(months = months, days = days, nanoseconds = microSeconds * 1000)
        }
        // https://github.com/postgres/postgres/blob/874d817baa160ca7e68bee6ccc9fc1848c56e750/src/backend/utils/adt/timestamp.c#L983
        is PgValue.Text -> DateTimePeriod.parse(value.text)
    }
}
