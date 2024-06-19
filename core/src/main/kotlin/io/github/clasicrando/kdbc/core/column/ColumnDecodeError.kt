package io.github.clasicrando.kdbc.core.column

import io.github.clasicrando.kdbc.core.exceptions.KdbcException
import kotlin.reflect.KType
import kotlin.reflect.typeOf

/** Exception thrown when a value cannot be decoded properly by the specified decoder */
class ColumnDecodeError(
    dataType: Int,
    typeName: String,
    columnName: String,
    decodeType: KType,
    reason: String,
    cause: Throwable?,
) : KdbcException(
    "Could not decode bytes into desired type. Actual Type: $typeName($dataType), " +
            "Column: '$columnName', " +
            "Desired Output: $decodeType" + if (reason.isNotBlank()) ", Reason: $reason" else "",
    cause,
)

/** Throw a [ColumnDecodeError] for the [kType] with the [type] */
fun columnDecodeError(
    kType: KType,
    type: ColumnMetadata,
    reason: String = "",
    cause: Throwable? = null,
): Nothing {
    throw ColumnDecodeError(
        dataType = type.dataType,
        typeName = type.typeName,
        columnName = type.fieldName,
        decodeType = kType,
        reason = reason,
        cause = cause,
    )
}

/** Throw a [ColumnDecodeError] for the type [T] with the [type] */
inline fun <reified T> columnDecodeError(
    type: ColumnMetadata,
    reason: String = "",
    cause: Throwable? = null,
): Nothing {
    throw ColumnDecodeError(
        dataType = type.dataType,
        typeName = type.typeName,
        columnName = type.fieldName,
        decodeType = typeOf<T>(),
        reason = reason,
        cause = cause,
    )
}

/** Evaluate the [check] parameter and if it's false throw a [ColumnDecodeError] */
inline fun <reified T> checkOrColumnDecodeError(
    check: Boolean,
    type: ColumnMetadata,
    reason: () -> String = { "" },
) {
    if (!check) {
        columnDecodeError<T>(type = type, reason = reason())
    }
}

/** Evaluate the [check] parameter and if it's false throw a [ColumnDecodeError] */
inline fun checkOrColumnDecodeError(
    check: Boolean,
    kType: KType,
    type: ColumnMetadata,
    reason: () -> String,
) {
    if (!check) {
        val text = reason()
        columnDecodeError(kType = kType, type = type, reason = text)
    }
}
