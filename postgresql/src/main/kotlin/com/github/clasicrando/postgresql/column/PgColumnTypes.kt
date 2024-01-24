package com.github.clasicrando.postgresql.column

@Suppress("ConstPropertyName")
object PgColumnTypes {
    const val Untyped = 0
    const val Char = 18
    const val CharArray = 1002
    const val Smallint = 21
    const val SmallintArray = 1005
    const val Integer = 23
    const val IntegerArray = 1007
    const val Bigint = 20
    const val BigintArray = 1016
    const val Numeric = 1700

    // Decimal is the same as Numeric on PostgreSQL
    const val NumericArray = 1231
    const val Real = 700
    const val RealArray = 1021
    const val Double = 701
    const val DoubleArray = 1022
    const val Bpchar = 1042
    const val BpcharArray = 1014
    const val Varchar = 1043

    // Char is the same as Varchar on PostgreSQL
    const val VarcharArray = 1015
    const val CharacterData = 13206
    const val CharacterDataArray = 13205
    const val Text = 25
    const val TextArray = 1009
    const val Json = 114
    const val JsonArray = 199
    const val Jsonb = 3802
    const val JsonbArray = 4072
    const val Date = 1082
    const val DateArray = 1182
    const val Time = 1083
    const val TimeArray = 1183
    const val TimeWithTimezone = 1266
    const val TimeWithTimezoneArray = 1270
    const val Timestamp = 1114
    const val TimestampArray = 1115
    const val TimestampWithTimezone = 1184
    const val TimestampWithTimezoneArray = 1185
    const val Interval = 1186
    const val IntervalArray = 1187
    const val Boolean = 16
    const val BooleanArray = 1000
    const val OID = 26
    const val OIDArray = 1028

    const val ByteA = 17
    const val ByteA_Array = 1001

    const val Money = 790
    const val MoneyArray = 791
    const val Name = 19
    const val NameArray = 1003
    const val UUID = 2950
    const val UUIDArray = 2951
    const val XML = 142
    const val XMLArray = 143

    const val Inet = 869
    const val InetArray = 1041
}
