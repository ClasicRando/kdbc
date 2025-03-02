package io.github.clasicrando.kdbc.core.query

import io.github.clasicrando.kdbc.core.result.DataRow
import io.github.clasicrando.kdbc.core.result.getAsNonNull
import io.github.clasicrando.kdbc.core.result.getBigDecimalNonNull
import io.github.clasicrando.kdbc.core.result.getBooleanNonNull
import io.github.clasicrando.kdbc.core.result.getDoubleNonNull
import io.github.clasicrando.kdbc.core.result.getFloatNonNull
import io.github.clasicrando.kdbc.core.result.getInstantNonNull
import io.github.clasicrando.kdbc.core.result.getIntNonNull
import io.github.clasicrando.kdbc.core.result.getLocalDateNonNull
import io.github.clasicrando.kdbc.core.result.getLocalDateTimeNonNull
import io.github.clasicrando.kdbc.core.result.getLocalTimeNonNull
import io.github.clasicrando.kdbc.core.result.getLongNonNull
import io.github.clasicrando.kdbc.core.result.getOffsetDateTimeNonNull
import io.github.clasicrando.kdbc.core.result.getShortNonNull
import io.github.clasicrando.kdbc.core.result.getStringNonNull
import java.math.BigDecimal
import java.time.Instant
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.LocalTime
import java.time.OffsetDateTime
import kotlin.uuid.Uuid

/** Standard [RowParser] for rows with a single [Boolean] field */
public object BooleanRowParser : RowParser<Boolean> {
    override fun fromRow(row: DataRow): Boolean = row.getBooleanNonNull(0)
}

/** Standard [RowParser] for rows with a single [Short] field */
public object ShortRowParser : RowParser<Short> {
    override fun fromRow(row: DataRow): Short = row.getShortNonNull(0)
}

/** Standard [RowParser] for rows with a single [Int] field */
public object IntRowParser : RowParser<Int> {
    override fun fromRow(row: DataRow): Int = row.getIntNonNull(0)
}

/** Standard [RowParser] for rows with a single [Long] field */
public object LongRowParser : RowParser<Long> {
    override fun fromRow(row: DataRow): Long = row.getLongNonNull(0)
}

/** Standard [RowParser] for rows with a single [Float] field */
public object FloatRowParser : RowParser<Float> {
    override fun fromRow(row: DataRow): Float = row.getFloatNonNull(0)
}

/** Standard [RowParser] for rows with a single [Double] field */
public object DoubleRowParser : RowParser<Double> {
    override fun fromRow(row: DataRow): Double = row.getDoubleNonNull(0)
}

/** Standard [RowParser] for rows with a single [BigDecimal] field */
public object BigDecimalRowParser : RowParser<BigDecimal> {
    override fun fromRow(row: DataRow): BigDecimal = row.getBigDecimalNonNull(0)
}

/** Standard [RowParser] for rows with a single [String] field */
public object StringRowParser : RowParser<String> {
    override fun fromRow(row: DataRow): String = row.getStringNonNull(0)
}

/** Standard [RowParser] for rows with a single [LocalTime] field */
public object LocalTimeRowParser : RowParser<LocalTime> {
    override fun fromRow(row: DataRow): LocalTime = row.getLocalTimeNonNull(0)
}

/** Standard [RowParser] for rows with a single [LocalDate] field */
public object LocalDateRowParser : RowParser<LocalDate> {
    override fun fromRow(row: DataRow): LocalDate = row.getLocalDateNonNull(0)
}

/** Standard [RowParser] for rows with a single [Instant] field */
public object InstantRowParser : RowParser<Instant> {
    override fun fromRow(row: DataRow): Instant = row.getInstantNonNull(0)
}

/** Standard [RowParser] for rows with a single [LocalDateTime] field */
public object LocalDateTimeRowParser : RowParser<LocalDateTime> {
    override fun fromRow(row: DataRow): LocalDateTime = row.getLocalDateTimeNonNull(0)
}

/** Standard [RowParser] for rows with a single [OffsetDateTime] field */
public object OffsetDateTimeRowParser : RowParser<OffsetDateTime> {
    override fun fromRow(row: DataRow): OffsetDateTime = row.getOffsetDateTimeNonNull(0)
}

/** Standard [RowParser] for rows with a single [Uuid] field */
public object UuidRowParser : RowParser<Uuid> {
    override fun fromRow(row: DataRow): Uuid = row.getAsNonNull(0)
}

/** Standard [RowParser] for rows with a single [java.util.UUID] field */
public object JavaUuidRowParser : RowParser<java.util.UUID> {
    override fun fromRow(row: DataRow): java.util.UUID = row.getAsNonNull(0)
}
