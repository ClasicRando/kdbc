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
import io.github.clasicrando.kdbc.postgresql.column.MacAddress8ArrayTypeDescription
import io.github.clasicrando.kdbc.postgresql.column.MacAddress8TypeDescription
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

    private fun <T : Any> checkAndDecode(
        index: Int,
        deserializer: PgTypeDescription<T>,
    ): T? {
        val pgValue = pgValues[index] ?: return null
        val columnPgType = pgValue.typeData.pgType
        return when (columnPgType.oid) {
            deserializer.pgType.oid -> deserializer.decode(pgValue)
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
        val pgType = getPgType(index)
        return when (pgType.oid) {
            PgType.BOOL -> checkAndDecode(index, BoolTypeDescription)
            PgType.BOOL_ARRAY -> checkAndDecode(index, BoolArrayTypeDescription)
            PgType.BOX -> checkAndDecode(index, BoxTypeDescription)
            PgType.BOX_ARRAY -> checkAndDecode(index, BoxArrayTypeDescription)
            PgType.BYTEA -> checkAndDecode(index, ByteaTypeDescription)
            PgType.BYTEA_ARRAY -> checkAndDecode(index, ByteaArrayTypeDescription)
            PgType.CHAR -> checkAndDecode(index, CharTypeDescription)
            PgType.CHAR_ARRAY -> checkAndDecode(index, CharArrayTypeDescription)
            PgType.CIRCLE -> checkAndDecode(index, CircleTypeDescription)
            PgType.CIRCLE_ARRAY -> checkAndDecode(index, CircleArrayTypeDescription)
            PgType.DATE -> checkAndDecode(index, DateTypeDescription)
            PgType.DATE_ARRAY -> checkAndDecode(index, DateArrayTypeDescription)
            PgType.DATERANGE -> checkAndDecode(index, DateRangeTypeDescription)
            PgType.DATERANGE_ARRAY -> checkAndDecode(index, DateRangeArrayTypeDescription)
            PgType.FLOAT4 -> checkAndDecode(index, RealTypeDescription)
            PgType.FLOAT4_ARRAY -> checkAndDecode(index, RealArrayTypeDescription)
            PgType.FLOAT8 -> checkAndDecode(index, DoublePrecisionTypeDescription)
            PgType.FLOAT8_ARRAY -> checkAndDecode(index, DoublePrecisionArrayTypeDescription)
            PgType.INET -> checkAndDecode(index, InetTypeDescription)
            PgType.INET_ARRAY -> checkAndDecode(index, InetArrayTypeDescription)
            PgType.CIDR -> checkAndDecode(index, CidrTypeDescription)
            PgType.CIDR_ARRAY -> checkAndDecode(index, CidrArrayTypeDescription)
            PgType.INT2 -> checkAndDecode(index, SmallIntTypeDescription)
            PgType.INT2_ARRAY -> checkAndDecode(index, SmallIntArrayTypeDescription)
            PgType.INT4 -> checkAndDecode(index, IntTypeDescription)
            PgType.OID -> checkAndDecode(index, OidTypeDescription)
            PgType.INT4_ARRAY -> checkAndDecode(index, IntArrayTypeDescription)
            PgType.OID_ARRAY -> checkAndDecode(index, OidArrayTypeDescription)
            PgType.INT4RANGE -> checkAndDecode(index, Int4RangeTypeDescription)
            PgType.INT4RANGE_ARRAY -> checkAndDecode(index, Int4RangeArrayTypeDescription)
            PgType.INT8 -> checkAndDecode(index, BigIntTypeDescription)
            PgType.INT8_ARRAY -> checkAndDecode(index, BigIntArrayTypeDescription)
            PgType.INT8RANGE -> checkAndDecode(index, Int8RangeTypeDescription)
            PgType.INT8RANGE_ARRAY -> checkAndDecode(index, Int8RangeArrayTypeDescription)
            PgType.INTERVAL -> checkAndDecode(index, IntervalTypeDescription)
            PgType.INTERVAL_ARRAY -> checkAndDecode(index, IntervalArrayTypeDescription)
            PgType.JSONB -> checkAndDecode(index, JsonbTypeDescription)
            PgType.JSONB_ARRAY -> checkAndDecode(index, JsonbArrayTypeDescription)
            PgType.JSON -> checkAndDecode(index, JsonTypeDescription)
            PgType.JSON_ARRAY -> checkAndDecode(index, JsonArrayTypeDescription)
            PgType.JSONPATH -> checkAndDecode(index, JsonPathTypeDescription)
            PgType.JSONPATH_ARRAY -> checkAndDecode(index, JsonPathArrayTypeDescription)
            PgType.LINE -> checkAndDecode(index, LineTypeDescription)
            PgType.LINE_ARRAY -> checkAndDecode(index, LineArrayTypeDescription)
            PgType.LSEG -> checkAndDecode(index, LineSegmentTypeDescription)
            PgType.LSEG_ARRAY -> checkAndDecode(index, LineSegmentArrayTypeDescription)
            PgType.MACADDR -> checkAndDecode(index, MacAddressTypeDescription)
            PgType.MACADDR8 -> checkAndDecode(index, MacAddress8TypeDescription)
            PgType.MACADDR_ARRAY -> checkAndDecode(index, MacAddressArrayTypeDescription)
            PgType.MACADDR8_ARRAY -> checkAndDecode(index, MacAddress8ArrayTypeDescription)
            PgType.MONEY -> checkAndDecode(index, MoneyTypeDescription)
            PgType.MONEY_ARRAY -> checkAndDecode(index, MoneyArrayTypeDescription)

            PgType.TEXT -> checkAndDecode(index, TextTypeDescription)
            PgType.VARCHAR -> checkAndDecode(index, VarcharTypeDescription)
            PgType.NAME -> checkAndDecode(index, NameTypeDescription)
            PgType.BPCHAR -> checkAndDecode(index, BpcharTypeDescription)
            PgType.XML -> checkAndDecode(index, XmlTypeDescription)
            PgType.TEXT_ARRAY -> checkAndDecode(index, TextArrayTypeDescription)
            PgType.VARCHAR_ARRAY -> checkAndDecode(index, VarcharArrayTypeDescription)
            PgType.NAME_ARRAY -> checkAndDecode(index, NameArrayTypeDescription)
            PgType.BPCHAR_ARRAY -> checkAndDecode(index, BpcharArrayTypeDescription)
            PgType.XML_ARRAY -> checkAndDecode(index, XmlArrayTypeDescription)

            PgType.NUMRANGE -> checkAndDecode(index, NumRangeTypeDescription)
            PgType.NUMRANGE_ARRAY -> checkAndDecode(index, NumRangeArrayTypeDescription)
            PgType.NUMERIC -> checkAndDecode(index, NumericTypeDescription)
            PgType.NUMERIC_ARRAY -> checkAndDecode(index, NumericArrayTypeDescription)
            PgType.PATH -> checkAndDecode(index, PathTypeDescription)
            PgType.PATH_ARRAY -> checkAndDecode(index, PathArrayTypeDescription)
            PgType.POINT -> checkAndDecode(index, PointTypeDescription)
            PgType.POINT_ARRAY -> checkAndDecode(index, PointArrayTypeDescription)
            PgType.POLYGON -> checkAndDecode(index, PolygonTypeDescription)
            PgType.POLYGON_ARRAY -> checkAndDecode(index, PolygonArrayTypeDescription)
            PgType.TIME -> checkAndDecode(index, TimeTypeDescription)
            PgType.TIME_ARRAY -> checkAndDecode(index, TimeArrayTypeDescription)
            PgType.TIMESTAMP -> checkAndDecode(index, TimestampTypeDescription)
            PgType.TIMESTAMP_ARRAY -> checkAndDecode(index, TimestampArrayTypeDescription)
            PgType.TIMESTAMPTZ -> checkAndDecode(index, TimestampTzTypeDescription)
            PgType.TIMESTAMPTZ_ARRAY -> checkAndDecode(index, TimestampTzArrayTypeDescription)
            PgType.TIMETZ -> checkAndDecode(index, TimeTzTypeDescription)
            PgType.TIMETZ_ARRAY -> checkAndDecode(index, TimeTzArrayTypeDescription)
            PgType.TSRANGE -> checkAndDecode(index, TsRangeTypeDescription)
            PgType.TSRANGE_ARRAY -> checkAndDecode(index, TsRangeArrayTypeDescription)
            PgType.TSTZRANGE -> checkAndDecode(index, TsTzRangeTypeDescription)
            PgType.TSTZRANGE_ARRAY -> checkAndDecode(index, TsTzRangeArrayTypeDescription)
            PgType.UUID -> checkAndDecode(index, UuidTypeDescription)
            PgType.UUID_ARRAY -> checkAndDecode(index, UuidArrayTypeDescription)
            PgType.VOID -> Unit
            PgType.RECORD, PgType.RECORD_ARRAY -> error("Cannot decode record/record[] types")
            PgType.UNKNOWN, PgType.UNSPECIFIED -> error("Backend doesn't know the data's type")
            PgType.BIT, PgType.BIT_ARRAY, PgType.VARBIT, PgType.VARBIT_ARRAY -> error("Bit types are not supported")
            else -> when (pgType) {
                is PgType.ByOid -> {
                    val typeDescription = customTypeDescriptionCache.getTypeDescription<Any>(pgType)
                        ?: error("Could not get type description from custom type cache")
                    return checkAndDecode(index, typeDescription)
                }
                else -> error("Could not find type description for PgType = $pgType")
            }
        }
    }

    override fun close() {
        rowBuffer?.close()
        pgValues = emptyArray()
    }
}
