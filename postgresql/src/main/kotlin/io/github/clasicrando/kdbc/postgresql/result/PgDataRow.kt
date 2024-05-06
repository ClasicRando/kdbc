package io.github.clasicrando.kdbc.postgresql.result

import io.github.clasicrando.kdbc.core.buffer.ByteReadBuffer
import io.github.clasicrando.kdbc.core.column.columnDecodeError
import io.github.clasicrando.kdbc.core.result.DataRow
import io.github.clasicrando.kdbc.postgresql.column.BigIntArrayTypeDescription
import io.github.clasicrando.kdbc.postgresql.column.BigIntTypeDescription
import io.github.clasicrando.kdbc.postgresql.column.BoolArrayTypeDescription
import io.github.clasicrando.kdbc.postgresql.column.BoolTypeDescription
import io.github.clasicrando.kdbc.postgresql.column.BoxArrayTypeDescription
import io.github.clasicrando.kdbc.postgresql.column.BoxTypeDescription
import io.github.clasicrando.kdbc.postgresql.column.BpcharArrayTypeDescription
import io.github.clasicrando.kdbc.postgresql.column.BpcharTypeDescription
import io.github.clasicrando.kdbc.postgresql.column.ByteaArrayTypeDescription
import io.github.clasicrando.kdbc.postgresql.column.ByteaTypeDescription
import io.github.clasicrando.kdbc.postgresql.column.CharArrayTypeDescription
import io.github.clasicrando.kdbc.postgresql.column.CharTypeDescription
import io.github.clasicrando.kdbc.postgresql.column.CidrArrayTypeDescription
import io.github.clasicrando.kdbc.postgresql.column.CidrTypeDescription
import io.github.clasicrando.kdbc.postgresql.column.CircleArrayTypeDescription
import io.github.clasicrando.kdbc.postgresql.column.CircleTypeDescription
import io.github.clasicrando.kdbc.postgresql.column.DateArrayTypeDescription
import io.github.clasicrando.kdbc.postgresql.column.DateRangeArrayTypeDescription
import io.github.clasicrando.kdbc.postgresql.column.DateRangeTypeDescription
import io.github.clasicrando.kdbc.postgresql.column.DateTypeDescription
import io.github.clasicrando.kdbc.postgresql.column.DoublePrecisionArrayTypeDescription
import io.github.clasicrando.kdbc.postgresql.column.DoublePrecisionTypeDescription
import io.github.clasicrando.kdbc.postgresql.column.InetArrayTypeDescription
import io.github.clasicrando.kdbc.postgresql.column.InetTypeDescription
import io.github.clasicrando.kdbc.postgresql.column.Int4RangeArrayTypeDescription
import io.github.clasicrando.kdbc.postgresql.column.Int4RangeTypeDescription
import io.github.clasicrando.kdbc.postgresql.column.Int8RangeArrayTypeDescription
import io.github.clasicrando.kdbc.postgresql.column.Int8RangeTypeDescription
import io.github.clasicrando.kdbc.postgresql.column.IntArrayTypeDescription
import io.github.clasicrando.kdbc.postgresql.column.IntTypeDescription
import io.github.clasicrando.kdbc.postgresql.column.IntervalArrayTypeDescription
import io.github.clasicrando.kdbc.postgresql.column.IntervalTypeDescription
import io.github.clasicrando.kdbc.postgresql.column.JsonArrayTypeDescription
import io.github.clasicrando.kdbc.postgresql.column.JsonPathArrayTypeDescription
import io.github.clasicrando.kdbc.postgresql.column.JsonPathTypeDescription
import io.github.clasicrando.kdbc.postgresql.column.JsonTypeDescription
import io.github.clasicrando.kdbc.postgresql.column.JsonbArrayTypeDescription
import io.github.clasicrando.kdbc.postgresql.column.JsonbTypeDescription
import io.github.clasicrando.kdbc.postgresql.column.LineArrayTypeDescription
import io.github.clasicrando.kdbc.postgresql.column.LineSegmentArrayTypeDescription
import io.github.clasicrando.kdbc.postgresql.column.LineSegmentTypeDescription
import io.github.clasicrando.kdbc.postgresql.column.LineTypeDescription
import io.github.clasicrando.kdbc.postgresql.column.MacAddressArrayTypeDescription
import io.github.clasicrando.kdbc.postgresql.column.MacAddressTypeDescription
import io.github.clasicrando.kdbc.postgresql.column.MoneyArrayTypeDescription
import io.github.clasicrando.kdbc.postgresql.column.MoneyTypeDescription
import io.github.clasicrando.kdbc.postgresql.column.NameArrayTypeDescription
import io.github.clasicrando.kdbc.postgresql.column.NameTypeDescription
import io.github.clasicrando.kdbc.postgresql.column.NumRangeArrayTypeDescription
import io.github.clasicrando.kdbc.postgresql.column.NumRangeTypeDescription
import io.github.clasicrando.kdbc.postgresql.column.NumericArrayTypeDescription
import io.github.clasicrando.kdbc.postgresql.column.NumericTypeDescription
import io.github.clasicrando.kdbc.postgresql.column.OidArrayTypeDescription
import io.github.clasicrando.kdbc.postgresql.column.OidTypeDescription
import io.github.clasicrando.kdbc.postgresql.column.PathArrayTypeDescription
import io.github.clasicrando.kdbc.postgresql.column.PathTypeDescription
import io.github.clasicrando.kdbc.postgresql.column.PgColumnDescription
import io.github.clasicrando.kdbc.postgresql.column.PgType
import io.github.clasicrando.kdbc.postgresql.column.PgTypeCache
import io.github.clasicrando.kdbc.postgresql.column.PgTypeDescription
import io.github.clasicrando.kdbc.postgresql.column.PgValue
import io.github.clasicrando.kdbc.postgresql.column.PointArrayTypeDescription
import io.github.clasicrando.kdbc.postgresql.column.PointTypeDescription
import io.github.clasicrando.kdbc.postgresql.column.PolygonArrayTypeDescription
import io.github.clasicrando.kdbc.postgresql.column.PolygonTypeDescription
import io.github.clasicrando.kdbc.postgresql.column.RealArrayTypeDescription
import io.github.clasicrando.kdbc.postgresql.column.RealTypeDescription
import io.github.clasicrando.kdbc.postgresql.column.SmallIntArrayTypeDescription
import io.github.clasicrando.kdbc.postgresql.column.SmallIntTypeDescription
import io.github.clasicrando.kdbc.postgresql.column.TextArrayTypeDescription
import io.github.clasicrando.kdbc.postgresql.column.TextTypeDescription
import io.github.clasicrando.kdbc.postgresql.column.TimeArrayTypeDescription
import io.github.clasicrando.kdbc.postgresql.column.TimeTypeDescription
import io.github.clasicrando.kdbc.postgresql.column.TimeTzArrayTypeDescription
import io.github.clasicrando.kdbc.postgresql.column.TimeTzTypeDescription
import io.github.clasicrando.kdbc.postgresql.column.TimestampArrayTypeDescription
import io.github.clasicrando.kdbc.postgresql.column.TimestampTypeDescription
import io.github.clasicrando.kdbc.postgresql.column.TimestampTzArrayTypeDescription
import io.github.clasicrando.kdbc.postgresql.column.TimestampTzTypeDescription
import io.github.clasicrando.kdbc.postgresql.column.TsRangeArrayTypeDescription
import io.github.clasicrando.kdbc.postgresql.column.TsRangeTypeDescription
import io.github.clasicrando.kdbc.postgresql.column.TsTzRangeArrayTypeDescription
import io.github.clasicrando.kdbc.postgresql.column.TsTzRangeTypeDescription
import io.github.clasicrando.kdbc.postgresql.column.UuidArrayTypeDescription
import io.github.clasicrando.kdbc.postgresql.column.UuidTypeDescription
import io.github.clasicrando.kdbc.postgresql.column.VarcharArrayTypeDescription
import io.github.clasicrando.kdbc.postgresql.column.VarcharTypeDescription
import io.github.clasicrando.kdbc.postgresql.column.XmlArrayTypeDescription
import io.github.clasicrando.kdbc.postgresql.column.XmlTypeDescription

/**
 * Postgresql specific implementation for a [DataRow]. Uses the [rowBuffer] to extract data
 * returned from the postgresql server.
 */
class PgDataRow(
    private val rowBuffer: ByteReadBuffer?,
    private var pgValues: Array<PgValue?>,
    private val columnMapping: List<PgColumnDescription>,
    private val customTypeDescriptionCache: PgTypeCache,
) : DataRow {

    /**
     * Check to ensure the [index] is valid for this row
     *
     * @throws IllegalArgumentException if the [index] can not be found in the [columnMapping]
     */
    private fun checkIndex(index: Int) {
        require(index in columnMapping.indices) {
            val range = columnMapping.indices
            "Index $index is not a valid index in this result. Values must be in $range"
        }
    }

    private fun getPgType(index: Int): PgType {
        checkIndex(index)
        return columnMapping[index].pgType
    }

    /**
     * Get a [PgValue] for the specified [index], returning null if the value sent from the server
     * was a database NULL. The format code of the column specified by [index] decides if the value
     * returned is a [PgValue.Text] or [PgValue.Binary].
     *
     * @throws IllegalArgumentException if the [index] can not be found in the [columnMapping]
     */
    private fun getPgValue(index: Int): PgValue? {
        checkIndex(index)
        return pgValues[index]
    }

    private fun <T : Any> checkAndDecode(
        index: Int,
        deserializer: PgTypeDescription<T>,
    ): T? {
        val pgValue = getPgValue(index) ?: return null
        return when (val columnPgType = pgValue.typeData.pgType) {
            deserializer.pgType -> deserializer.decode(pgValue)
            else -> columnDecodeError<Any>(
                type = pgValue.typeData,
                reason = "PgType of deserializer does not match the current column. " +
                        "$columnPgType != ${deserializer.pgType}"
            )
        }
    }

    override fun indexFromColumn(column: String): Int {
        val result = columnMapping.indexOfFirst { c -> c.fieldName == column }
        if (result >= 0) {
            return result
        }
        val columns = columnMapping.withIndex().joinToString { (i, c) -> "$i->${c.fieldName}" }
        error("Could not find column in mapping. Column = '$column', columns = $columns")
    }

    override fun get(index: Int): Any? {
        return when (val pgType = getPgType(index)) {
            PgType.Bool -> checkAndDecode(index, BoolTypeDescription)
            PgType.BoolArray -> checkAndDecode(index, BoolArrayTypeDescription)
            PgType.Box -> checkAndDecode(index, BoxTypeDescription)
            PgType.BoxArray -> checkAndDecode(index, BoxArrayTypeDescription)
            PgType.Bytea -> checkAndDecode(index, ByteaTypeDescription)
            PgType.ByteaArray -> checkAndDecode(index, ByteaArrayTypeDescription)
            PgType.Char -> checkAndDecode(index, CharTypeDescription)
            PgType.CharArray -> checkAndDecode(index, CharArrayTypeDescription)
            PgType.Circle -> checkAndDecode(index, CircleTypeDescription)
            PgType.CircleArray -> checkAndDecode(index, CircleArrayTypeDescription)
            PgType.Date -> checkAndDecode(index, DateTypeDescription)
            PgType.DateArray -> checkAndDecode(index, DateArrayTypeDescription)
            PgType.DateRange -> checkAndDecode(index, DateRangeTypeDescription)
            PgType.DateRangeArray -> checkAndDecode(index, DateRangeArrayTypeDescription)
            PgType.Float4 -> checkAndDecode(index, RealTypeDescription)
            PgType.Float4Array -> checkAndDecode(index, RealArrayTypeDescription)
            PgType.Float8 -> checkAndDecode(index, DoublePrecisionTypeDescription)
            PgType.Float8Array -> checkAndDecode(index, DoublePrecisionArrayTypeDescription)
            PgType.Inet -> checkAndDecode(index, InetTypeDescription)
            PgType.InetArray -> checkAndDecode(index, InetArrayTypeDescription)
            PgType.Cidr -> checkAndDecode(index, CidrTypeDescription)
            PgType.CidrArray -> checkAndDecode(index, CidrArrayTypeDescription)
            PgType.Int2 -> checkAndDecode(index, SmallIntTypeDescription)
            PgType.Int2Array -> checkAndDecode(index, SmallIntArrayTypeDescription)
            PgType.Int4 -> checkAndDecode(index, IntTypeDescription)
            PgType.Oid -> checkAndDecode(index, OidTypeDescription)
            PgType.Int4Array -> checkAndDecode(index, IntArrayTypeDescription)
            PgType.OidArray -> checkAndDecode(index, OidArrayTypeDescription)
            PgType.Int4Range -> checkAndDecode(index, Int4RangeTypeDescription)
            PgType.Int4RangeArray -> checkAndDecode(index, Int4RangeArrayTypeDescription)
            PgType.Int8 -> checkAndDecode(index, BigIntTypeDescription)
            PgType.Int8Array -> checkAndDecode(index, BigIntArrayTypeDescription)
            PgType.Int8Range -> checkAndDecode(index, Int8RangeTypeDescription)
            PgType.Int8RangeArray -> checkAndDecode(index, Int8RangeArrayTypeDescription)
            PgType.Interval -> checkAndDecode(index, IntervalTypeDescription)
            PgType.IntervalArray -> checkAndDecode(index, IntervalArrayTypeDescription)
            PgType.Jsonb -> checkAndDecode(index, JsonbTypeDescription)
            PgType.JsonbArray -> checkAndDecode(index, JsonbArrayTypeDescription)
            PgType.Json -> checkAndDecode(index, JsonTypeDescription)
            PgType.JsonArray -> checkAndDecode(index, JsonArrayTypeDescription)
            PgType.Jsonpath -> checkAndDecode(index, JsonPathTypeDescription)
            PgType.JsonpathArray -> checkAndDecode(index, JsonPathArrayTypeDescription)
            PgType.Line -> checkAndDecode(index, LineTypeDescription)
            PgType.LineArray -> checkAndDecode(index, LineArrayTypeDescription)
            PgType.LineSegment -> checkAndDecode(index, LineSegmentTypeDescription)
            PgType.LineSegmentArray -> checkAndDecode(index, LineSegmentArrayTypeDescription)
            PgType.Macaddr -> checkAndDecode(index, MacAddressTypeDescription)
            PgType.Macaddr8 -> checkAndDecode(index, MacAddressTypeDescription)
            PgType.Macaddr8Array -> checkAndDecode(index, MacAddressArrayTypeDescription)
            PgType.MacaddrArray -> checkAndDecode(index, MacAddressArrayTypeDescription)
            PgType.Money -> checkAndDecode(index, MoneyTypeDescription)
            PgType.MoneyArray -> checkAndDecode(index, MoneyArrayTypeDescription)

            PgType.Text -> checkAndDecode(index, TextTypeDescription)
            PgType.Varchar -> checkAndDecode(index, VarcharTypeDescription)
            PgType.Name -> checkAndDecode(index, NameTypeDescription)
            PgType.Bpchar -> checkAndDecode(index, BpcharTypeDescription)
            PgType.Xml -> checkAndDecode(index, XmlTypeDescription)
            PgType.TextArray -> checkAndDecode(index, TextArrayTypeDescription)
            PgType.VarcharArray -> checkAndDecode(index, VarcharArrayTypeDescription)
            PgType.NameArray -> checkAndDecode(index, NameArrayTypeDescription)
            PgType.BpcharArray -> checkAndDecode(index, BpcharArrayTypeDescription)
            PgType.XmlArray -> checkAndDecode(index, XmlArrayTypeDescription)

            PgType.NumRange -> checkAndDecode(index, NumRangeTypeDescription)
            PgType.NumRangeArray -> checkAndDecode(index, NumRangeArrayTypeDescription)
            PgType.Numeric -> checkAndDecode(index, NumericTypeDescription)
            PgType.NumericArray -> checkAndDecode(index, NumericArrayTypeDescription)
            PgType.Path -> checkAndDecode(index, PathTypeDescription)
            PgType.PathArray -> checkAndDecode(index, PathArrayTypeDescription)
            PgType.Point -> checkAndDecode(index, PointTypeDescription)
            PgType.PointArray -> checkAndDecode(index, PointArrayTypeDescription)
            PgType.Polygon -> checkAndDecode(index, PolygonTypeDescription)
            PgType.PolygonArray -> checkAndDecode(index, PolygonArrayTypeDescription)
            PgType.Time -> checkAndDecode(index, TimeTypeDescription)
            PgType.TimeArray -> checkAndDecode(index, TimeArrayTypeDescription)
            PgType.Timestamp -> checkAndDecode(index, TimestampTypeDescription)
            PgType.TimestampArray -> checkAndDecode(index, TimestampArrayTypeDescription)
            PgType.Timestamptz -> checkAndDecode(index, TimestampTzTypeDescription)
            PgType.TimestamptzArray -> checkAndDecode(index, TimestampTzArrayTypeDescription)
            PgType.Timetz -> checkAndDecode(index, TimeTzTypeDescription)
            PgType.TimetzArray -> checkAndDecode(index, TimeTzArrayTypeDescription)
            PgType.TsRange -> checkAndDecode(index, TsRangeTypeDescription)
            PgType.TsRangeArray -> checkAndDecode(index, TsRangeArrayTypeDescription)
            PgType.TstzRange -> checkAndDecode(index, TsTzRangeTypeDescription)
            PgType.TstzRangeArray -> checkAndDecode(index, TsTzRangeArrayTypeDescription)
            PgType.Uuid -> checkAndDecode(index, UuidTypeDescription)
            PgType.UuidArray -> checkAndDecode(index, UuidArrayTypeDescription)
            PgType.Void -> Unit
            is PgType.ByOid -> {
                val typeDescription = customTypeDescriptionCache.getTypeDescription<Any>(pgType)
                    ?: error("Could not get type description from custom type cache")
                return checkAndDecode(index, typeDescription)
            }
            PgType.Record, PgType.RecordArray -> error("Cannot decode record/record[] types")
            PgType.Unknown, PgType.Unspecified -> error("Backend doesn't know the data's type")
            PgType.Bit, PgType.BitArray, PgType.Varbit, PgType.VarbitArray -> error("Bit types are not supported")
        }
    }

    override fun release() {
        rowBuffer?.release()
        pgValues = emptyArray()
    }
}
