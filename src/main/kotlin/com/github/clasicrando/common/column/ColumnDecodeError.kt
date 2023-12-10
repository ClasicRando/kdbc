package com.github.clasicrando.common.column

/** Exception thrown when a value cannot be decoded properly by the specified [DbType] decode */
class ColumnDecodeError(
    dataType: Int,
    typeName: String,
    value: String,
) : Throwable(
    """
    Could not decode bytes into desired type.
    Type: $dataType($typeName)
    Value: $value
    """.trimIndent()
)

/** Throw a [ColumnDecodeError] with the [type] and [bytes] (decoded as a UTF-8 byte string) */
fun columnDecodeError(type: ColumnData, bytes: ByteArray): Nothing {
    throw ColumnDecodeError(type.dataType, type.name, bytes.toString(charset = Charsets.UTF_8))
}

/** Throw a [ColumnDecodeError] with the [type] and [string] */
fun columnDecodeError(type: ColumnData, string: String): Nothing {
    throw ColumnDecodeError(type.dataType, type.name, string)
}
