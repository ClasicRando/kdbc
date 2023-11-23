package com.github.clasicrando.common.column

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

fun columnDecodeError(type: ColumnData, bytes: ByteArray): Nothing {
    throw ColumnDecodeError(type.dataType, type.name, bytes.toString(charset = Charsets.UTF_8))
}

fun columnDecodeError(type: ColumnData, string: String): Nothing {
    throw ColumnDecodeError(type.dataType, type.name, string)
}
