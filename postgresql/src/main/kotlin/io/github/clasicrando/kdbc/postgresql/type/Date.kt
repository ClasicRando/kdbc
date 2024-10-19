package io.github.clasicrando.kdbc.postgresql.type

import io.github.clasicrando.kdbc.core.buffer.ByteWriteBuffer
import io.github.clasicrando.kdbc.core.column.columnDecodeError
import io.github.clasicrando.kdbc.core.datetime.InvalidDateString
import io.github.clasicrando.kdbc.core.datetime.tryFromString
import io.github.clasicrando.kdbc.postgresql.column.PgValue
import kotlinx.datetime.DateTimeUnit
import kotlinx.datetime.LocalDate
import kotlinx.datetime.daysUntil
import kotlinx.datetime.plus
import kotlinx.datetime.toJavaLocalDate
import java.time.format.DateTimeParseException
import java.time.temporal.ChronoUnit
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
internal object LocalDateTypeDescription : PgTypeDescription<LocalDate>(
    dbType = PgType.Date,
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
     * @throws io.github.clasicrando.kdbc.core.column.ColumnDecodeError if the text value cannot be
     * parsed into a [LocalDate]
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
 * Zero date within a postgresql database. Date values sent as binary are always an offset of days
 * from this [LocalDate]
 */
private val postgresEpochDateJTime = postgresEpochDate.toJavaLocalDate()

/**
 * Implementation of a [PgTypeDescription] for the [java.time.LocalDate] type. This maps to the
 * `date` type in a postgresql database.
 */
internal object JLocalDateTypeDescription : PgTypeDescription<java.time.LocalDate>(
    dbType = PgType.Date,
    kType = typeOf<java.time.LocalDate>(),
) {
    /**
     * Writes the number of days until the [postgresEpochDateJTime] as an [Int] to the argument
     * buffer.
     *
     * [pg source code](https://github.com/postgres/postgres/blob/874d817baa160ca7e68bee6ccc9fc1848c56e750/src/backend/utils/adt/date.c#L209)
     */
    override fun encode(value: java.time.LocalDate, buffer: ByteWriteBuffer) {
        val difference = postgresEpochDateJTime.until(value, ChronoUnit.DAYS)
        buffer.writeInt(difference.toInt())
    }

    /**
     * Reads an [Int] from the value and use that as the number of days since the
     * [postgresEpochDateJTime].
     *
     * [pg source code](https://github.com/postgres/postgres/blob/874d817baa160ca7e68bee6ccc9fc1848c56e750/src/backend/utils/adt/date.c#L231)
     */
    override fun decodeBytes(value: PgValue.Binary): java.time.LocalDate {
        val days = value.bytes.readInt()
        return postgresEpochDateJTime.plus(days.toLong(), ChronoUnit.DAYS)
    }

    /**
     * Attempt to parse the [String] into a [java.time.LocalDate].
     *
     * [pg source code](https://github.com/postgres/postgres/blob/874d817baa160ca7e68bee6ccc9fc1848c56e750/src/backend/utils/adt/date.c#L184)
     *
     * @throws io.github.clasicrando.kdbc.core.column.ColumnDecodeError if the text value cannot be
     * parsed into a [java.time.LocalDate]
     */
    override fun decodeText(value: PgValue.Text): java.time.LocalDate {
        return try {
            java.time.LocalDate.parse(value.text)
        } catch (ex: DateTimeParseException) {
            columnDecodeError<java.time.LocalDate>(type = value.typeData, cause = ex)
        }
    }
}
