package com.github.clasicrando.postgresql.column

import com.github.clasicrando.common.datetime.tryFromString
import com.github.clasicrando.postgresql.type.PgTimeTz
import kotlinx.datetime.LocalTime
import kotlinx.datetime.UtcOffset
import kotlin.time.DurationUnit
import kotlin.time.toDuration

val timeTypeEncoder = PgTypeEncoder<LocalTime>(PgType.Time) { value, buffer ->
    val microSeconds = value
        .toNanosecondOfDay()
        .toDuration(DurationUnit.NANOSECONDS)
        .inWholeMicroseconds
    buffer.writeLong(microSeconds)
}

val timeTypeDecoder = PgTypeDecoder { value ->
    when (value) {
        is PgValue.Binary -> {
            val microSeconds = value.bytes.readLong()
            LocalTime.fromNanosecondOfDay(microSeconds * 1000)
        }
        is PgValue.Text -> LocalTime.tryFromString(value.text).getOrThrow()
    }
}

val timeTzTypeEncoder = PgTypeEncoder<PgTimeTz>(PgType.Timetz) { value, buffer ->
    timeTypeEncoder.encode(value.time, buffer)
    buffer.writeInt(value.offset.totalSeconds)
}

val timeTzTypeDecoder = PgTypeDecoder { value ->
    when (value) {
        is PgValue.Binary -> {
            val microSeconds = value.bytes.readLong()
            val offsetSeconds = value.bytes.readInt()
            PgTimeTz(
                time = LocalTime.fromNanosecondOfDay(microSeconds * 1000),
                offset = UtcOffset(seconds = offsetSeconds),
            )
        }
        is PgValue.Text -> PgTimeTz.fromString(value.text)
    }
}
