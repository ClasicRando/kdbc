package com.github.clasicrando.common.column

import com.github.clasicrando.common.exceptions.KdbcException
import kotlin.reflect.KType
import kotlin.reflect.typeOf

/** Exception thrown when a value cannot be decoded properly by the specified decoder */
class ColumnDecodeError(
    dataType: Int,
    typeName: String,
    columnName: String,
    decodeType: KType,
    reason: String,
) : KdbcException(
    "Could not decode bytes into desired type. Actual Type: $typeName($dataType), " +
            "Column: '$columnName', " +
            "Desired Output: $decodeType" + if (reason.isNotBlank()) ", Reason: $reason" else ""
)

/** Throw a [ColumnDecodeError] for the [kType] with the [type] */
fun columnDecodeError(kType: KType, type: ColumnData, reason: String = ""): Nothing {
    throw ColumnDecodeError(type.dataType, type.typeName, type.fieldName, kType, reason)
}

/** Throw a [ColumnDecodeError] for the type [T] with the [type] */
inline fun <reified T> columnDecodeError(type: ColumnData, reason: String = ""): Nothing {
    throw ColumnDecodeError(type.dataType, type.typeName, type.fieldName, typeOf<T>(), reason)
}

/** Evaluate the [check] parameter and if it's false throw a [ColumnDecodeError] */
inline fun <reified T> checkOrColumnDecodeError(
    check: Boolean,
    type: ColumnData,
    reason: () -> String = { "" },
) {
    if (!check) {
        columnDecodeError<T>(type, reason())
    }
}

/** Evaluate the [check] parameter and if it's false throw a [ColumnDecodeError] */
inline fun checkOrColumnDecodeError(
    check: Boolean,
    kType: KType,
    type: ColumnData,
    reason: () -> String,
) {
    if (!check) {
        val text = reason()
        columnDecodeError(kType, type, text)
    }
}
