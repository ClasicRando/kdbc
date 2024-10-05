package io.github.clasicrando.kdbc.core.query

import com.ionspin.kotlin.bignum.decimal.BigDecimal
import io.github.clasicrando.kdbc.core.datetime.DateTime
import io.github.clasicrando.kdbc.core.result.DataRow
import io.github.clasicrando.kdbc.core.result.getAsNonNull
import kotlinx.datetime.Instant
import kotlin.uuid.Uuid

/**
 * Standard [RowParser] for rows with a single [Boolean] field
 */
object BooleanRowParser : RowParser<Boolean> {
    override fun fromRow(row: DataRow): Boolean = row.getAsNonNull(0)
}

/**
 * Standard [RowParser] for rows with a single [Short] field
 */
object ShortRowParser : RowParser<Short> {
    override fun fromRow(row: DataRow): Short = row.getAsNonNull(0)
}

/**
 * Standard [RowParser] for rows with a single [Int] field
 */
object IntRowParser : RowParser<Int> {
    override fun fromRow(row: DataRow): Int = row.getAsNonNull(0)
}

/**
 * Standard [RowParser] for rows with a single [Long] field
 */
object LongRowParser : RowParser<Long> {
    override fun fromRow(row: DataRow): Long = row.getAsNonNull(0)
}

/**
 * Standard [RowParser] for rows with a single [Float] field
 */
object FloatRowParser : RowParser<Float> {
    override fun fromRow(row: DataRow): Float = row.getAsNonNull(0)
}

/**
 * Standard [RowParser] for rows with a single [Double] field
 */
object DoubleRowParser : RowParser<Double> {
    override fun fromRow(row: DataRow): Double = row.getAsNonNull(0)
}

/**
 * Standard [RowParser] for rows with a single [BigDecimal] field
 */
object BigDecimalRowParser : RowParser<BigDecimal> {
    override fun fromRow(row: DataRow): BigDecimal = row.getAsNonNull(0)
}

/**
 * Standard [RowParser] for rows with a single [String] field
 */
object StringRowParser : RowParser<String> {
    override fun fromRow(row: DataRow): String = row.getAsNonNull(0)
}

/**
 * Standard [RowParser] for rows with a single [Instant] field
 */
object InstantRowParser : RowParser<Instant> {
    override fun fromRow(row: DataRow): Instant = row.getAsNonNull(0)
}

/**
 * Standard [RowParser] for rows with a single [DateTime] field
 */
object DateTimeRowParser : RowParser<DateTime> {
    override fun fromRow(row: DataRow): DateTime = row.getAsNonNull(0)
}

/**
 * Standard [RowParser] for rows with a single [Uuid] field
 */
object UuidRowParser : RowParser<Uuid> {
    override fun fromRow(row: DataRow): Uuid = row.getAsNonNull(0)
}
