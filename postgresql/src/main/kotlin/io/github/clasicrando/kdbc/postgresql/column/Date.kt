package io.github.clasicrando.kdbc.postgresql.column

import io.github.clasicrando.kdbc.core.buffer.ByteWriteBuffer
import io.github.clasicrando.kdbc.core.column.ColumnDecodeError
import io.github.clasicrando.kdbc.core.column.columnDecodeError
import io.github.clasicrando.kdbc.core.datetime.InvalidDateString
import io.github.clasicrando.kdbc.core.datetime.tryFromString
import kotlinx.datetime.DateTimeUnit
import kotlinx.datetime.LocalDate
import kotlinx.datetime.daysUntil
import kotlinx.datetime.plus
import kotlin.reflect.typeOf

/**
 * Zero date within a postgresql database. Date values sent as binary are always an offset of days
 * from this [LocalDate]
 */
private val postgresEpochDate = LocalDate(year = 2000, monthNumber = 1, dayOfMonth = 1)

/**
 * Implementation of a [PgTypeDescription] for the [LocalDate] type. This maps to the `date` type
 * in a postgresql database.
 */
object DateTypeDescription : PgTypeDescription<LocalDate>(
    pgType = PgType.Date,
    kType = typeOf<LocalDate>(),
) {
    /**
     * Writes the number of days until the [postgresEpochDate] as an [Int] to the argument buffer.
     *
     * [pg source code](https://github.com/postgres/postgres/blob/874d817baa160ca7e68bee6ccc9fc1848c56e750/src/backend/utils/adt/date.c#L209)
     */
    override fun encode(value: LocalDate, buffer: ByteWriteBuffer) {
        val difference = postgresEpochDate.daysUntil(value)
        buffer.writeInt(difference)
    }

    /**
     * Reads an [Int] from the value and use that as the number of days since the
     * [postgresEpochDate].
     *
     * [pg source code](https://github.com/postgres/postgres/blob/874d817baa160ca7e68bee6ccc9fc1848c56e750/src/backend/utils/adt/date.c#L231)
     */
    override fun decodeBytes(value: PgValue.Binary): LocalDate {
        val days = value.bytes.readInt()
        return postgresEpochDate.plus(days, DateTimeUnit.DAY)
    }

    /**
     * Attempt to parse the [String] into a [LocalDate].
     *
     * [pg source code](https://github.com/postgres/postgres/blob/874d817baa160ca7e68bee6ccc9fc1848c56e750/src/backend/utils/adt/date.c#L184)
     *
     * @throws ColumnDecodeError if the text value cannot be parsed into a [LocalDate]
     */
    override fun decodeText(value: PgValue.Text): LocalDate {
        return try {
            LocalDate.tryFromString(value.text)
        } catch (ex: InvalidDateString) {
            columnDecodeError<LocalDate>(type = value.typeData, cause = ex)
        }
    }
}

/**
 * Implementation of an [ArrayTypeDescription] for [LocalDate]. This maps to the `date[]` type in a
 * postgresql database.
 */
object DateArrayTypeDescription : ArrayTypeDescription<LocalDate>(
    pgType = PgType.DateArray,
    innerType = DateTypeDescription,
)
