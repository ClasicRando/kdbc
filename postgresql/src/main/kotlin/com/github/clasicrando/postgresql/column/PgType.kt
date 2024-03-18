package com.github.clasicrando.postgresql.column

/** Sealed class representing all covered postgresql types and their Oid */
sealed class PgType(
    /**
     * Oid value representing the unique identifier of a type within a given postgresql database.
     * For all types implemented as a data object of [PgType], the type is static for any
     * postgresql database. User defined types will not be static, so they are defined as [ByOid]
     * or [ByName].
     */
    val oid: Int
) {
    /** [PgType] representing the `boolean` type */
    data object Bool : PgType(BOOL)
    /** [PgType] representing the `boolean[]` type */
    data object BoolArray : PgType(BOOL_ARRAY)
    /** [PgType] representing the `bytea` type */
    data object Bytea : PgType(BYTEA)
    /** [PgType] representing the `bytea[]` type */
    data object ByteaArray : PgType(BYTEA_ARRAY)
    /** [PgType] representing the `"char"` type */
    data object Char : PgType(CHAR)
    /** [PgType] representing the `"char"[]` type */
    data object CharArray : PgType(CHAR_ARRAY)
    /** [PgType] representing the `name` type */
    data object Name : PgType(NAME)
    /** [PgType] representing the `name[]` type */
    data object NameArray : PgType(NAME_ARRAY)
    /** [PgType] representing the `bigint` type */
    data object Int8 : PgType(INT8)
    /** [PgType] representing the `bigint[]` type */
    data object Int8Array : PgType(INT8_ARRAY)
    /** [PgType] representing the `smallint` type */
    data object Int2 : PgType(INT2)
    /** [PgType] representing the `smallint[]` type */
    data object Int2Array : PgType(INT2_ARRAY)
    /** [PgType] representing the `int` type */
    data object Int4 : PgType(INT4)
    /** [PgType] representing the `int[]` type */
    data object Int4Array : PgType(INT4_ARRAY)
    /** [PgType] representing the `text` type */
    data object Text : PgType(TEXT)
    /** [PgType] representing the `text[]` type */
    data object TextArray : PgType(TEXT_ARRAY)
    /** [PgType] representing the `oid` type */
    data object Oid : PgType(OID)
    /** [PgType] representing the `oid[]` type */
    data object OidArray : PgType(OID_ARRAY)
    /** [PgType] representing the `json` type */
    data object Json : PgType(JSON)
    /** [PgType] representing the `json[]` type */
    data object JsonArray : PgType(JSON_ARRAY)
    /** [PgType] representing the `point` type */
    data object Point : PgType(POINT)
    /** [PgType] representing the `point[]` type */
    data object PointArray : PgType(POINT_ARRAY)
    /** [PgType] representing the `lseg` type */
    data object LineSegment : PgType(LSEG)
    /** [PgType] representing the `lseg[]` type */
    data object LineSegmentArray : PgType(LSEG_ARRAY)
    /** [PgType] representing the `path` type */
    data object Path : PgType(PATH)
    /** [PgType] representing the `path[]` type */
    data object PathArray : PgType(PATH_ARRAY)
    /** [PgType] representing the `box` type */
    data object Box : PgType(BOX)
    /** [PgType] representing the `box[]` type */
    data object BoxArray : PgType(BOX_ARRAY)
    /** [PgType] representing the `polygon` type */
    data object Polygon : PgType(POLYGON)
    /** [PgType] representing the `polygon[]` type */
    data object PolygonArray : PgType(POLYGON_ARRAY)
    /** [PgType] representing the `line` type */
    data object Line : PgType(LINE)
    /** [PgType] representing the `line[]` type */
    data object LineArray : PgType(LINE_ARRAY)
    /** [PgType] representing the `cidr` type */
    data object Cidr : PgType(CIDR)
    /** [PgType] representing the `cidr[]` type */
    data object CidrArray : PgType(CIDR_ARRAY)
    /** [PgType] representing the `real` type */
    data object Float4 : PgType(FLOAT4)
    /** [PgType] representing the `real[]` type */
    data object Float4Array : PgType(FLOAT4_ARRAY)
    /** [PgType] representing the `double precision` type */
    data object Float8 : PgType(FLOAT8)
    /** [PgType] representing the `double precision[]` type */
    data object Float8Array : PgType(FLOAT8_ARRAY)
    /** [PgType] representing the `unknown` type */
    data object Unknown : PgType(UNKNOWN)
    /** [PgType] representing the `circle` type */
    data object Circle : PgType(CIRCLE)
    /** [PgType] representing the `circle[]` type */
    data object CircleArray : PgType(CIRCLE_ARRAY)
    /** [PgType] representing the `macaddr8` type */
    data object Macaddr8 : PgType(MACADDR8)
    /** [PgType] representing the `macaddr8[]` type */
    data object Macaddr8Array : PgType(MACADDR8_ARRAY)
    /** [PgType] representing the `macaddr[]` type */
    data object Macaddr : PgType(MACADDR)
    /** [PgType] representing the `macaddr[]` type */
    data object MacaddrArray : PgType(MACADDR_ARRAY)
    /** [PgType] representing the `inet` type */
    data object Inet : PgType(INET)
    /** [PgType] representing the `inet[]` type */
    data object InetArray : PgType(INET_ARRAY)
    /** [PgType] representing the `bpchar` type */
    data object Bpchar : PgType(BPCHAR)
    /** [PgType] representing the `bpchar[]` type */
    data object BpcharArray : PgType(BPCHAR_ARRAY)
    /** [PgType] representing the `varchar` type */
    data object Varchar : PgType(VARCHAR)
    /** [PgType] representing the `varchar[]` type */
    data object VarcharArray : PgType(VARCHAR_ARRAY)
    /** [PgType] representing the `date` type */
    data object Date : PgType(DATE)
    /** [PgType] representing the `date[]` type */
    data object DateArray : PgType(DATE_ARRAY)
    /** [PgType] representing the `time` type */
    data object Time : PgType(TIME)
    /** [PgType] representing the `time[]` type */
    data object TimeArray : PgType(TIME_ARRAY)
    /** [PgType] representing the `timestamp without time zone` type */
    data object Timestamp : PgType(TIMESTAMP)
    /** [PgType] representing the `timestamp without time zone[]` type */
    data object TimestampArray : PgType(TIMESTAMP_ARRAY)
    /** [PgType] representing the `timestamp with time zone` type */
    data object Timestamptz : PgType(TIMESTAMPTZ)
    /** [PgType] representing the `timestamp with time zone[]` type */
    data object TimestamptzArray : PgType(TIMESTAMPTZ_ARRAY)
    /** [PgType] representing the `interval` type */
    data object Interval : PgType(INTERVAL)
    /** [PgType] representing the `interval[]` type */
    data object IntervalArray : PgType(INTERVAL_ARRAY)
    /** [PgType] representing the `time with time zone` type */
    data object Timetz : PgType(TIMETZ)
    /** [PgType] representing the `time with time zone[]` type */
    data object TimetzArray : PgType(TIMETZ_ARRAY)
    /** [PgType] representing the `bit(n)` type */
    data object Bit : PgType(BIT)
    /** [PgType] representing the `bit(n)[]` type */
    data object BitArray : PgType(BIT_ARRAY)
    /** [PgType] representing the `bit varying(n)` type */
    data object Varbit : PgType(VARBIT)
    /** [PgType] representing the `bit varying(n)[]` type */
    data object VarbitArray : PgType(VARBIT_ARRAY)
    /** [PgType] representing the `numeric` type */
    data object Numeric : PgType(NUMERIC)
    /** [PgType] representing the `numeric[]` type */
    data object NumericArray : PgType(NUMERIC_ARRAY)
    /** [PgType] representing the `record` type */
    data object Record : PgType(RECORD)
    /** [PgType] representing the `record[]` type */
    data object RecordArray : PgType(RECORD_ARRAY)
    /** [PgType] representing the `uuid` type */
    data object Uuid : PgType(UUID)
    /** [PgType] representing the `uuid[]` type */
    data object UuidArray : PgType(UUID_ARRAY)
    /** [PgType] representing the `jsonb` type */
    data object Jsonb : PgType(JSONB)
    /** [PgType] representing the `jsonb[]` type */
    data object JsonbArray : PgType(JSONB_ARRAY)
    /** [PgType] representing the `int4range` type */
    data object Int4Range : PgType(INT4RANGE)
    /** [PgType] representing the `int4range[]` type */
    data object Int4RangeArray : PgType(INT4RANGE_ARRAY)
    /** [PgType] representing the `numrange` type */
    data object NumRange : PgType(NUMRANGE)
    /** [PgType] representing the `numrange[]` type */
    data object NumRangeArray : PgType(NUMRANGE_ARRAY)
    /** [PgType] representing the `tsrange` type */
    data object TsRange : PgType(TSRANGE)
    /** [PgType] representing the `tsrange[]` type */
    data object TsRangeArray : PgType(TSRANGE_ARRAY)
    /** [PgType] representing the `tstzrange` type */
    data object TstzRange : PgType(TSTZRANGE)
    /** [PgType] representing the `tstzrange[]` type */
    data object TstzRangeArray : PgType(TSTZRANGE_ARRAY)
    /** [PgType] representing the `daterange` type */
    data object DateRange : PgType(DATERANGE)
    /** [PgType] representing the `daterange[]` type */
    data object DateRangeArray : PgType(DATERANGE_ARRAY)
    /** [PgType] representing the `int8range` type */
    data object Int8Range : PgType(INT8RANGE)
    /** [PgType] representing the `int8range[]` type */
    data object Int8RangeArray : PgType(INT8RANGE_ARRAY)
    /** [PgType] representing the `jsonpath` type */
    data object Jsonpath : PgType(JSONPATH)
    /** [PgType] representing the `jsonpath[]` type */
    data object JsonpathArray : PgType(JSONPATH_ARRAY)
    /** [PgType] representing the `money` type */
    data object Money : PgType(MONEY)
    /** [PgType] representing the `money[]` type */
    data object MoneyArray : PgType(MONEY_ARRAY)
    /** [PgType] representing the `xml` type */
    data object Xml : PgType(XML)
    /** [PgType] representing the `xml[]` type */
    data object XmlArray : PgType(XML_ARRAY)
    /** [PgType] representing the `void` type */
    data object Void : PgType(VOID)
    /** Unspecified type. Used in executing prepared queries where the type is not known */
    data object Unspecified : PgType(0)
    /** Define a [PgType] by a known [oid] */
    class ByOid(oid: Int) : PgType(oid) {
        override fun toString(): String {
            return "PgType.ByOid(oid=$oid)"
        }
    }
    /**
     * Define a [PgType] by a [name]. This is the preferred method of defining the [PgType] for a
     * user defined type since the [oid] can vary database to database. [oid] defaults to [UNKNOWN]
     * but this instance should be replaced with a known [oid] during registration in the
     * [PgTypeRegistry].
     */
    class ByName(val name: String, oid: Int = UNKNOWN) : PgType(oid) {
        override fun toString(): String {
            return "PgType.ByName(name=$name, oid=$oid)"
        }
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

        /**
         * Attempt to the match the [oid] to a static type, otherwise, [ByOid] is returned storing
         * the [oid] within.
         */
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
                LSEG -> LineSegment
                LSEG_ARRAY -> LineSegmentArray
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
