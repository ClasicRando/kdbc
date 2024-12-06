package io.github.clasicrando.kdbc.postgresql.type

import io.github.clasicrando.kdbc.core.column.columnDecodeError
import io.github.clasicrando.kdbc.postgresql.column.PgValue
import java.time.LocalDate
import java.time.format.DateTimeParseException
import java.time.temporal.ChronoUnit
import kotlin.reflect.typeOf
import kotlinx.io.Sink

/**
 * Zero date within a postgresql database. Date values sent as binary are always an offset of days
 * from this [LocalDate]
 */
private val postgresEpochDateJTime = LocalDate.of(2000, 1, 1)

/**
 * Implementation of a [PgTypeDescription] for the [java.time.LocalDate] type. This maps to the
 * `date` type in a postgresql database.
 */
internal object JLocalDateTypeDescription :
    PgTypeDescription<LocalDate>(dbType = PgType.Date, kType = typeOf<LocalDate>()) {
    /**
     * Writes the number of days until the [postgresEpochDateJTime] as an [Int] to the argument
     * buffer.
     *
     * [pg source
     * code](https://github.com/postgres/postgres/blob/874d817baa160ca7e68bee6ccc9fc1848c56e750/src/backend/utils/adt/date.c#L209)
     */
    override fun encode(value: LocalDate, buffer: Sink) {
        val difference = postgresEpochDateJTime.until(value, ChronoUnit.DAYS)
        buffer.writeInt(difference.toInt())
    }

    /**
     * Reads an [Int] from the value and use that as the number of days since the
     * [postgresEpochDateJTime].
     *
     * [pg source
     * code](https://github.com/postgres/postgres/blob/874d817baa160ca7e68bee6ccc9fc1848c56e750/src/backend/utils/adt/date.c#L231)
     */
    override fun decodeBytes(value: PgValue.Binary): LocalDate {
        val days = value.bytes.readInt()
        return postgresEpochDateJTime.plus(days.toLong(), ChronoUnit.DAYS)
    }

    /**
     * Attempt to parse the [String] into a [java.time.LocalDate].
     *
     * [pg source
     * code](https://github.com/postgres/postgres/blob/874d817baa160ca7e68bee6ccc9fc1848c56e750/src/backend/utils/adt/date.c#L184)
     *
     * @throws io.github.clasicrando.kdbc.core.column.ColumnDecodeError if the text value cannot be
     *   parsed into a [java.time.LocalDate]
     */
    override fun decodeText(value: PgValue.Text): LocalDate {
        return try {
            LocalDate.parse(value.text)
        } catch (ex: DateTimeParseException) {
            columnDecodeError<LocalDate>(type = value.typeData, cause = ex)
        }
    }
}
