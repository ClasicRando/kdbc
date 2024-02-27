package com.github.clasicrando.postgresql.column

import com.github.clasicrando.common.datetime.tryFromString
import com.github.clasicrando.postgresql.type.PgTimeTz
import kotlinx.datetime.LocalTime
import kotlinx.datetime.UtcOffset
import kotlin.time.DurationUnit
import kotlin.time.toDuration

// https://github.com/postgres/postgres/blob/874d817baa160ca7e68bee6ccc9fc1848c56e750/src/backend/utils/adt/date.c#L1521
val timeTypeEncoder = PgTypeEncoder<LocalTime>(PgType.Time) { value, buffer ->
    val microSeconds = value
        .toNanosecondOfDay()
        .toDuration(DurationUnit.NANOSECONDS)
        .inWholeMicroseconds
    buffer.writeLong(microSeconds)
}

val timeTypeDecoder = PgTypeDecoder { value ->
    when (value) {
        // https://github.com/postgres/postgres/blob/874d817baa160ca7e68bee6ccc9fc1848c56e750/src/backend/utils/adt/date.c#L1547
        is PgValue.Binary -> {
            val microSeconds = value.bytes.readLong()
            LocalTime.fromNanosecondOfDay(microSeconds * 1000)
        }
        // https://github.com/postgres/postgres/blob/874d817baa160ca7e68bee6ccc9fc1848c56e750/src/backend/utils/adt/date.c#L1501
        is PgValue.Text -> LocalTime.tryFromString(value.text).getOrThrow()
    }
}

// https://github.com/postgres/postgres/blob/874d817baa160ca7e68bee6ccc9fc1848c56e750/src/backend/utils/adt/date.c#L2335
val timeTzTypeEncoder = PgTypeEncoder<PgTimeTz>(PgType.Timetz) { value, buffer ->
    timeTypeEncoder.encode(value.time, buffer)
    // Offset from postgres treats west of UTC as positive which is the opposite of UtcOffset
    buffer.writeInt(value.offset.totalSeconds * -1)
}

val timeTzTypeDecoder = PgTypeDecoder { value ->
    when (value) {
        // https://github.com/postgres/postgres/blob/874d817baa160ca7e68bee6ccc9fc1848c56e750/src/backend/utils/adt/date.c#L2371
        is PgValue.Binary -> {
            val microSeconds = value.bytes.readLong()
            // Offset from postgres treats west of UTC as positive which is the opposite of
            // UtcOffset
            val offsetSeconds = value.bytes.readInt() * -1
            PgTimeTz(
                time = LocalTime.fromNanosecondOfDay(microSeconds * 1000),
                offset = UtcOffset(seconds = offsetSeconds),
            )
        }
        // https://github.com/postgres/postgres/blob/874d817baa160ca7e68bee6ccc9fc1848c56e750/src/backend/utils/adt/date.c#L2314
        is PgValue.Text -> PgTimeTz.fromString(value.text)
    }
}
