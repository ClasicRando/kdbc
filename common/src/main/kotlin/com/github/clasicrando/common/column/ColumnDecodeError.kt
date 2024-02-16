package com.github.clasicrando.common.column

import kotlin.reflect.KType
import kotlin.reflect.typeOf

/** Exception thrown when a value cannot be decoded properly by the specified decoder */
class ColumnDecodeError(
    dataType: Int,
    typeName: String,
    columnName: String,
    decodeType: KType,
) : Throwable(
    "Could not decode bytes into desired type. Input: $dataType($typeName), " +
            "Column: '$columnName', " +
            "Desired Output: $decodeType"
)

/** Throw a [ColumnDecodeError] for the [kType] with the [type] */
fun columnDecodeError(kType: KType, type: ColumnData): Nothing {
    throw ColumnDecodeError(type.dataType, type.typeName, type.fieldName, kType)
}

/** Throw a [ColumnDecodeError] for the type [T] with the [type] */
inline fun <reified T> columnDecodeError(type: ColumnData): Nothing {
    throw ColumnDecodeError(type.dataType, type.typeName, type.fieldName, typeOf<T>())
}

inline fun <reified T> checkOrColumnDecodeError(check: Boolean, type: ColumnData) {
    if (!check) {
        columnDecodeError<T>(type)
    }
}
