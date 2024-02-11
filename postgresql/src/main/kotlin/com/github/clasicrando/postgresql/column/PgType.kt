package com.github.clasicrando.postgresql.column

sealed class PgType(val oid: Int?) {
    data object Bool : PgType(BOOL)
    data object BoolArray : PgType(BOOL_ARRAY)
    data object Bytea : PgType(BYTEA)
    data object ByteaArray : PgType(BYTEA_ARRAY)
    data object Char : PgType(CHAR)
    data object CharArray : PgType(CHAR_ARRAY)
    data object Name : PgType(NAME)
    data object NameArray : PgType(NAME_ARRAY)
    data object Int8 : PgType(INT8)
    data object Int8Array : PgType(INT8_ARRAY)
    data object Int2 : PgType(INT2)
    data object Int2Array : PgType(INT2_ARRAY)
    data object Int4 : PgType(INT4)
    data object Int4Array : PgType(INT4_ARRAY)
    data object Text : PgType(TEXT)
    data object TextArray : PgType(TEXT_ARRAY)
    data object Oid : PgType(OID)
    data object OidArray : PgType(OID_ARRAY)
    data object Json : PgType(JSON)
    data object JsonArray : PgType(JSON_ARRAY)
    data object Point : PgType(POINT)
    data object PointArray : PgType(POINT_ARRAY)
    data object Lseg : PgType(LSEG)
    data object LsegArray : PgType(LSEG_ARRAY)
    data object Path : PgType(PATH)
    data object PathArray : PgType(PATH_ARRAY)
    data object Box : PgType(BOX)
    data object BoxArray : PgType(BOX_ARRAY)
    data object Polygon : PgType(POLYGON)
    data object PolygonArray : PgType(POLYGON_ARRAY)
    data object Line : PgType(LINE)
    data object LineArray : PgType(LINE_ARRAY)
    data object Cidr : PgType(CIDR)
    data object CidrArray : PgType(CIDR_ARRAY)
    data object Float4 : PgType(FLOAT4)
    data object Float4Array : PgType(FLOAT4_ARRAY)
    data object Float8 : PgType(FLOAT8)
    data object Float8Array : PgType(FLOAT8_ARRAY)
    data object Unknown : PgType(UNKNOWN)
    data object Circle : PgType(CIRCLE)
    data object CircleArray : PgType(CIRCLE_ARRAY)
    data object Macaddr8 : PgType(MACADDR8)
    data object Macaddr8Array : PgType(MACADDR8_ARRAY)
    data object Macaddr : PgType(MACADDR)
    data object MacaddrArray : PgType(MACADDR_ARRAY)
    data object Inet : PgType(INET)
    data object InetArray : PgType(INET_ARRAY)
    data object Bpchar : PgType(BPCHAR)
    data object BpcharArray : PgType(BPCHAR_ARRAY)
    data object Varchar : PgType(VARCHAR)
    data object VarcharArray : PgType(VARCHAR_ARRAY)
    data object Date : PgType(DATE)
    data object DateArray : PgType(DATE_ARRAY)
    data object Time : PgType(TIME)
    data object TimeArray : PgType(TIME_ARRAY)
    data object Timestamp : PgType(TIMESTAMP)
    data object TimestampArray : PgType(TIMESTAMP_ARRAY)
    data object Timestamptz : PgType(TIMESTAMPTZ)
    data object TimestamptzArray : PgType(TIMESTAMPTZ_ARRAY)
    data object Interval : PgType(INTERVAL)
    data object IntervalArray : PgType(INTERVAL_ARRAY)
    data object Timetz : PgType(TIMETZ)
    data object TimetzArray : PgType(TIMETZ_ARRAY)
    data object Bit : PgType(BIT)
    data object BitArray : PgType(BIT_ARRAY)
    data object Varbit : PgType(VARBIT)
    data object VarbitArray : PgType(VARBIT_ARRAY)
    data object Numeric : PgType(NUMERIC)
    data object NumericArray : PgType(NUMERIC_ARRAY)
    data object Record : PgType(RECORD)
    data object RecordArray : PgType(RECORD_ARRAY)
    data object Uuid : PgType(UUID)
    data object UuidArray : PgType(UUID_ARRAY)
    data object Jsonb : PgType(JSONB)
    data object JsonbArray : PgType(JSONB_ARRAY)
    data object Int4Range : PgType(INT4RANGE)
    data object Int4RangeArray : PgType(INT4RANGE_ARRAY)
    data object NumRange : PgType(NUMRANGE)
    data object NumRangeArray : PgType(NUMRANGE_ARRAY)
    data object TsRange : PgType(TSRANGE)
    data object TsRangeArray : PgType(TSRANGE_ARRAY)
    data object TstzRange : PgType(TSTZRANGE)
    data object TstzRangeArray : PgType(TSTZRANGE_ARRAY)
    data object DateRange : PgType(DATERANGE)
    data object DateRangeArray : PgType(DATERANGE_ARRAY)
    data object Int8Range : PgType(INT8RANGE)
    data object Int8RangeArray : PgType(INT8RANGE_ARRAY)
    data object Jsonpath : PgType(JSONPATH)
    data object JsonpathArray : PgType(JSONPATH_ARRAY)
    data object Money : PgType(MONEY)
    data object MoneyArray : PgType(MONEY_ARRAY)
    data object Xml : PgType(MONEY)
    data object XmlArray : PgType(MONEY_ARRAY)
    data object Void : PgType(VOID)
    class ByOid(oid: Int?) : PgType(oid) {
        override fun toString(): String {
            return "PgType.ByOid(oid=$oid)"
        }
    }
    class ByName(val name: String, oid: Int? = null) : PgType(oid) {
        override fun toString(): String {
            return "PgType.ByName(name=$name, oid=$oid)"
        }
    }

    fun oidOrUnknown(): Int {
        return this.oid ?: Unknown.oid!!
    }
    
    companion object {
        const val BOOL = 16
        const val BOOL_ARRAY = 1000
        const val BYTEA = 17
        const val BYTEA_ARRAY = 1001
        const val CHAR = 18
        const val CHAR_ARRAY = 1002
        const val NAME = 19
        const val NAME_ARRAY = 1003
        const val INT2 = 21
        const val INT2_ARRAY = 1005
        const val INT4 = 23
        const val INT4_ARRAY = 1007
        const val INT8 = 20
        const val INT8_ARRAY = 1016
        const val TEXT = 25
        const val TEXT_ARRAY = 1009
        const val OID = 26
        const val OID_ARRAY = 1028
        const val JSON = 114
        const val JSON_ARRAY = 199
        const val POINT = 600
        const val POINT_ARRAY = 1017
        const val LSEG = 601
        const val LSEG_ARRAY = 1018
        const val PATH = 602
        const val PATH_ARRAY = 1019
        const val BOX = 603
        const val BOX_ARRAY = 1020
        const val POLYGON = 604
        const val POLYGON_ARRAY = 1027
        const val LINE = 628
        const val LINE_ARRAY = 629
        const val CIDR = 650
        const val CIDR_ARRAY = 651
        const val FLOAT4 = 700
        const val FLOAT4_ARRAY = 1021
        const val FLOAT8 = 701
        const val FLOAT8_ARRAY = 1022
        const val UNKNOWN = 705
        const val CIRCLE = 718
        const val CIRCLE_ARRAY = 719
        const val MACADDR8 = 774
        const val MACADDR8_ARRAY = 775
        const val MACADDR = 829
        const val MACADDR_ARRAY = 1040
        const val INET = 869
        const val INET_ARRAY = 1041
        const val BPCHAR = 1042
        const val BPCHAR_ARRAY = 1014
        const val VARCHAR = 1043
        const val VARCHAR_ARRAY = 1015
        const val DATE = 1082
        const val DATE_ARRAY = 1182
        const val TIME = 1083
        const val TIME_ARRAY = 1183
        const val TIMESTAMP = 1114
        const val TIMESTAMP_ARRAY = 1115
        const val TIMESTAMPTZ = 1184
        const val TIMESTAMPTZ_ARRAY = 1185
        const val INTERVAL = 1186
        const val INTERVAL_ARRAY = 1187
        const val TIMETZ = 1266
        const val TIMETZ_ARRAY = 1270
        const val BIT = 1560
        const val BIT_ARRAY = 1561
        const val VARBIT = 1562
        const val VARBIT_ARRAY = 1563
        const val NUMERIC = 1700
        const val NUMERIC_ARRAY = 1231
        const val RECORD = 2249
        const val RECORD_ARRAY = 2287
        const val UUID = 2950
        const val UUID_ARRAY = 2951
        const val JSONB = 3802
        const val JSONB_ARRAY = 3807
        const val INT4RANGE = 3904
        const val INT4RANGE_ARRAY = 3905
        const val NUMRANGE = 3906
        const val NUMRANGE_ARRAY = 3907
        const val TSRANGE = 3908
        const val TSRANGE_ARRAY = 3909
        const val TSTZRANGE = 3910
        const val TSTZRANGE_ARRAY = 3911
        const val DATERANGE = 3912
        const val DATERANGE_ARRAY = 3913
        const val INT8RANGE = 3926
        const val INT8RANGE_ARRAY = 3927
        const val JSONPATH = 4072
        const val JSONPATH_ARRAY = 4073
        const val MONEY = 790
        const val MONEY_ARRAY = 791
        const val XML = 142
        const val XML_ARRAY = 143
        const val VOID = 2278

        fun fromOid(oid: Int): PgType {
            return when (oid) {
                BOOL -> Bool
                BOOL_ARRAY -> BoolArray
                BYTEA -> Bytea
                BYTEA_ARRAY -> ByteaArray
                CHAR -> Char
                CHAR_ARRAY -> CharArray
                NAME -> Name
                NAME_ARRAY -> NameArray
                INT8 -> Int8
                INT8_ARRAY -> Int8Array
                INT2 -> Int2
                INT2_ARRAY -> Int2Array
                INT4 -> Int4
                INT4_ARRAY -> Int4Array
                TEXT -> Text
                TEXT_ARRAY -> TextArray
                OID -> Oid
                OID_ARRAY -> OidArray
                JSON -> Json
                JSON_ARRAY -> JsonArray
                POINT -> Point
                POINT_ARRAY -> PointArray
                LSEG -> Lseg
                LSEG_ARRAY -> LsegArray
                PATH -> Path
                PATH_ARRAY -> PathArray
                BOX -> Box
                BOX_ARRAY -> BoxArray
                POLYGON -> Polygon
                POLYGON_ARRAY -> PolygonArray
                LINE -> Line
                LINE_ARRAY -> LineArray
                CIDR -> Cidr
                CIDR_ARRAY -> CidrArray
                FLOAT4 -> Float4
                FLOAT4_ARRAY -> Float4Array
                FLOAT8 -> Float8
                FLOAT8_ARRAY -> Float8Array
                UNKNOWN -> Unknown
                CIRCLE -> Circle
                CIRCLE_ARRAY -> CircleArray
                MACADDR8 -> Macaddr8
                MACADDR8_ARRAY -> Macaddr8Array
                MACADDR -> Macaddr
                MACADDR_ARRAY -> MacaddrArray
                INET -> Inet
                INET_ARRAY -> InetArray
                BPCHAR -> Bpchar
                BPCHAR_ARRAY -> BpcharArray
                VARCHAR -> Varchar
                VARCHAR_ARRAY -> VarcharArray
                DATE -> Date
                DATE_ARRAY -> DateArray
                TIME -> Time
                TIME_ARRAY -> TimeArray
                TIMESTAMP -> Timestamp
                TIMESTAMP_ARRAY -> TimestampArray
                TIMESTAMPTZ -> Timestamptz
                TIMESTAMPTZ_ARRAY -> TimestamptzArray
                INTERVAL -> Interval
                INTERVAL_ARRAY -> IntervalArray
                TIMETZ -> Timetz
                TIMETZ_ARRAY -> TimetzArray
                BIT -> Bit
                BIT_ARRAY -> BitArray
                VARBIT -> Varbit
                VARBIT_ARRAY -> VarbitArray
                NUMERIC -> Numeric
                NUMERIC_ARRAY -> NumericArray
                RECORD -> Record
                RECORD_ARRAY -> RecordArray
                UUID -> Uuid
                UUID_ARRAY -> UuidArray
                JSONB -> Jsonb
                JSONB_ARRAY -> JsonbArray
                INT4RANGE -> Int4Range
                INT4RANGE_ARRAY -> Int4RangeArray
                NUMRANGE -> NumRange
                NUMRANGE_ARRAY -> NumRangeArray
                TSRANGE -> TsRange
                TSRANGE_ARRAY -> TsRangeArray
                TSTZRANGE -> TstzRange
                TSTZRANGE_ARRAY -> TstzRangeArray
                DATERANGE -> DateRange
                DATERANGE_ARRAY -> DateRangeArray
                INT8RANGE -> Int8Range
                INT8RANGE_ARRAY -> Int8RangeArray
                JSONPATH -> Jsonpath
                JSONPATH_ARRAY -> JsonpathArray
                MONEY -> Money
                MONEY_ARRAY -> MoneyArray
                XML -> Xml
                XML_ARRAY -> XmlArray
                VOID -> Void
                else -> ByOid(oid)
            }
        }
    }
}
