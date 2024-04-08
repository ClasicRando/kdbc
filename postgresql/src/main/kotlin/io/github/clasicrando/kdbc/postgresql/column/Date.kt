package io.github.clasicrando.kdbc.postgresql.column

import io.github.clasicrando.kdbc.core.column.ColumnDecodeError
import io.github.clasicrando.kdbc.core.column.columnDecodeError
import io.github.clasicrando.kdbc.core.datetime.InvalidDateString
import io.github.clasicrando.kdbc.core.datetime.tryFromString
import kotlinx.datetime.DateTimeUnit
import kotlinx.datetime.LocalDate
import kotlinx.datetime.daysUntil
import kotlinx.datetime.plus

/**
 * Zero date within a postgresql database. Date values sent as binary are always an offset of days
 * from this [LocalDate]
 */
private val postgresEpochDate = LocalDate(year = 2000, monthNumber = 1, dayOfMonth = 1)

/**
 * Implementation of a [PgTypeEncoder] for the [LocalDate] type. This maps to the `date` type in a
 * postgresql database. The encoder writes the number of days until the [postgresEpochDate] as an
 * [Int] to the argument buffer.
 *
 * [pg source code](https://github.com/postgres/postgres/blob/874d817baa160ca7e68bee6ccc9fc1848c56e750/src/backend/utils/adt/date.c#L209)
 */
val dateTypeEncoder = PgTypeEncoder<LocalDate>(PgType.Date) { value, buffer ->
    val difference = postgresEpochDate.daysUntil(value)
    buffer.writeInt(difference)
}

/**
 * Implementation of a [PgTypeDecoder] for the [LocalDate] type. This maps to the `date` type in a
 * postgresql database.
 *
 * ### Binary
 * Reads an [Int] from the value and use that as the number of days since the [postgresEpochDate].
 *
 * ### Text
 * Attempt to parse the [String] into a [LocalDate].
 *
 * [pg source code binary](https://github.com/postgres/postgres/blob/874d817baa160ca7e68bee6ccc9fc1848c56e750/src/backend/utils/adt/date.c#L231)
 * [pg source code text](https://github.com/postgres/postgres/blob/874d817baa160ca7e68bee6ccc9fc1848c56e750/src/backend/utils/adt/date.c#L184)
 *
 * @throws ColumnDecodeError if the text value cannot be parsed into a [LocalDate]
 */
val dateTypeDecoder = PgTypeDecoder { value ->
    when (value) {
        is PgValue.Binary -> {
            val days = value.bytes.readInt()
            postgresEpochDate.plus(days, DateTimeUnit.DAY)
        }
        is PgValue.Text -> try {
            LocalDate.tryFromString(value.text)
        } catch (ex: InvalidDateString) {
            columnDecodeError<LocalDate>(type = value.typeData, reason = ex.message ?: "")
        }
    }
}
