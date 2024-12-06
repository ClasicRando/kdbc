package io.github.clasicrando.kdbc.core.query

import io.github.clasicrando.kdbc.core.result.DataRow
import io.github.clasicrando.kdbc.core.result.getAsNonNull
import java.math.BigDecimal
import java.time.Instant
import java.time.LocalDateTime
import java.time.OffsetDateTime
import kotlin.uuid.Uuid

/** Standard [RowParser] for rows with a single [Boolean] field */
public object BooleanRowParser : RowParser<Boolean> {
    override fun fromRow(row: DataRow): Boolean = row.getAsNonNull(0)
}

/** Standard [RowParser] for rows with a single [Short] field */
public object ShortRowParser : RowParser<Short> {
    override fun fromRow(row: DataRow): Short = row.getAsNonNull(0)
}

/** Standard [RowParser] for rows with a single [Int] field */
public object IntRowParser : RowParser<Int> {
    override fun fromRow(row: DataRow): Int = row.getAsNonNull(0)
}

/** Standard [RowParser] for rows with a single [Long] field */
public object LongRowParser : RowParser<Long> {
    override fun fromRow(row: DataRow): Long = row.getAsNonNull(0)
}

/** Standard [RowParser] for rows with a single [Float] field */
public object FloatRowParser : RowParser<Float> {
    override fun fromRow(row: DataRow): Float = row.getAsNonNull(0)
}

/** Standard [RowParser] for rows with a single [Double] field */
public object DoubleRowParser : RowParser<Double> {
    override fun fromRow(row: DataRow): Double = row.getAsNonNull(0)
}

/** Standard [RowParser] for rows with a single [BigDecimal] field */
public object BigDecimalRowParser : RowParser<BigDecimal> {
    override fun fromRow(row: DataRow): BigDecimal = row.getAsNonNull(0)
}

/** Standard [RowParser] for rows with a single [String] field */
public object StringRowParser : RowParser<String> {
    override fun fromRow(row: DataRow): String = row.getAsNonNull(0)
}

/** Standard [RowParser] for rows with a single [Instant] field */
public object InstantRowParser : RowParser<Instant> {
    override fun fromRow(row: DataRow): Instant = row.getAsNonNull(0)
}

/** Standard [RowParser] for rows with a single [LocalDateTime] field */
public object JavaLocalDateTimeRowParser : RowParser<LocalDateTime> {
    override fun fromRow(row: DataRow): LocalDateTime = row.getAsNonNull(0)
}

/** Standard [RowParser] for rows with a single [OffsetDateTime] field */
public object OffsetDateTimeRowParser : RowParser<OffsetDateTime> {
    override fun fromRow(row: DataRow): OffsetDateTime = row.getAsNonNull(0)
}

/** Standard [RowParser] for rows with a single [Uuid] field */
public object UuidRowParser : RowParser<Uuid> {
    override fun fromRow(row: DataRow): Uuid = row.getAsNonNull(0)
}

/** Standard [RowParser] for rows with a single [java.util.UUID] field */
public object JavaUuidRowParser : RowParser<java.util.UUID> {
    override fun fromRow(row: DataRow): java.util.UUID = row.getAsNonNull(0)
}
