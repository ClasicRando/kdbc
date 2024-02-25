package com.github.clasicrando.postgresql.column

import com.github.clasicrando.common.datetime.tryFromString
import kotlinx.datetime.DateTimeUnit
import kotlinx.datetime.LocalDate
import kotlinx.datetime.daysUntil
import kotlinx.datetime.plus

private val postgresEpochDate = LocalDate(year = 2000, monthNumber = 1, dayOfMonth = 1)

val dateTypeEncoder = PgTypeEncoder<LocalDate>(PgType.Date) { value, buffer ->
    val difference = postgresEpochDate.daysUntil(value)
    buffer.writeInt(difference)
}

val dateTypeDecoder = PgTypeDecoder { value ->
    when (value) {
        is PgValue.Binary -> {
            val days = value.bytes.readInt()
            postgresEpochDate.plus(days, DateTimeUnit.DAY)
        }
        is PgValue.Text -> LocalDate.tryFromString(value.text).getOrThrow()
    }
}
