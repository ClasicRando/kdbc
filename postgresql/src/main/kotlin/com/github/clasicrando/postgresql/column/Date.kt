package com.github.clasicrando.postgresql.column

import com.github.clasicrando.common.datetime.tryFromString
import kotlinx.datetime.DateTimeUnit
import kotlinx.datetime.LocalDate
import kotlinx.datetime.daysUntil
import kotlinx.datetime.plus

private val postgresEpochDate = LocalDate(year = 2000, monthNumber = 1, dayOfMonth = 1)

// https://github.com/postgres/postgres/blob/874d817baa160ca7e68bee6ccc9fc1848c56e750/src/backend/utils/adt/date.c#L209
val dateTypeEncoder = PgTypeEncoder<LocalDate>(PgType.Date) { value, buffer ->
    val difference = postgresEpochDate.daysUntil(value)
    buffer.writeInt(difference)
}

val dateTypeDecoder = PgTypeDecoder { value ->
    when (value) {
        // https://github.com/postgres/postgres/blob/874d817baa160ca7e68bee6ccc9fc1848c56e750/src/backend/utils/adt/date.c#L231
        is PgValue.Binary -> {
            val days = value.bytes.readInt()
            postgresEpochDate.plus(days, DateTimeUnit.DAY)
        }
        // https://github.com/postgres/postgres/blob/874d817baa160ca7e68bee6ccc9fc1848c56e750/src/backend/utils/adt/date.c#L184
        is PgValue.Text -> LocalDate.tryFromString(value.text)
    }
}
