package io.github.clasicrando.kdbc.core.column

import io.github.clasicrando.kdbc.core.exceptions.KdbcException
import kotlin.reflect.KType

/** [KdbcException] thrown when a value cannot be extracted from a row as the required type */
class ColumnExtractError(type: KType, value: Any) : KdbcException(
    "Expected column to be of type $type, but got a value of $value (${value::class})",
)
