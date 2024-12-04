package io.github.clasicrando.kdbc.mysql.type

import io.github.clasicrando.kdbc.mysql.exceptions.MySqlException

private const val DECIMAL = 0x00
private const val TINY = 0x01
private const val SHORT = 0x02
private const val LONG = 0x03
private const val FLOAT = 0x04
private const val DOUBLE = 0x05
private const val NULL = 0x06
private const val TIMESTAMP = 0x07
private const val LONG_LONG = 0x08
private const val INT_24 = 0x09
private const val DATE = 0x0a
private const val TIME = 0x0b
private const val DATETIME = 0x0c
private const val YEAR = 0x0d
private const val VARCHAR = 0x0f
private const val BIT = 0x10
private const val JSON = 0xf5
private const val NEW_DECIMAL = 0xf6
private const val ENUM = 0xf7
private const val SET = 0xf8
private const val TINY_BLOB = 0xf9
private const val MEDIUM_BLOB = 0xfa
private const val LONG_BLOB = 0xfb
private const val BLOB = 0xfc
private const val VAR_STRING = 0xfd
private const val STRING = 0xfe
private const val GEOMETRY = 0xff

/** All natively supported types in MySQL */
public enum class MySqlType(
    /** internal type code */
    public val inner: Int
) {
    Decimal(DECIMAL),
    Tiny(TINY),
    Short(SHORT),
    Long(LONG),
    Float(FLOAT),
    Double(DOUBLE),
    Null(NULL),
    Timestamp(TIMESTAMP),
    LongLong(LONG_LONG),
    Int24(INT_24),
    Date(DATE),
    Time(TIME),
    Datetime(DATETIME),
    Year(YEAR),
    Varchar(VARCHAR),
    Bit(BIT),
    Json(JSON),
    NewDecimal(NEW_DECIMAL),
    Enum(ENUM),
    Set(SET),
    TinyBlob(TINY_BLOB),
    MediumBlob(MEDIUM_BLOB),
    LongBlob(LONG_BLOB),
    Blob(BLOB),
    VarString(VAR_STRING),
    String(STRING),
    Geometry(GEOMETRY);

    public companion object {
        /**
         * Return the [MySqlType] for this [code]
         *
         * @throws MySqlException if the type code cannot be matched to a known type
         */
        public fun from(code: Int): MySqlType =
            when (code) {
                DECIMAL -> Decimal
                TINY -> Tiny
                SHORT -> Short
                LONG -> Long
                FLOAT -> Float
                DOUBLE -> Double
                NULL -> Null
                TIMESTAMP -> Timestamp
                LONG_LONG -> LongLong
                INT_24 -> Int24
                DATE -> Date
                TIME -> Time
                DATETIME -> Datetime
                YEAR -> Year
                VARCHAR -> Varchar
                BIT -> Bit
                JSON -> Json
                NEW_DECIMAL -> NewDecimal
                ENUM -> Enum
                SET -> Set
                TINY_BLOB -> TinyBlob
                MEDIUM_BLOB -> MediumBlob
                LONG_BLOB -> LongBlob
                BLOB -> Blob
                VAR_STRING -> VarString
                STRING -> String
                GEOMETRY -> Geometry
                else -> throw MySqlException("Invalid type code of $code")
            }
    }
}
