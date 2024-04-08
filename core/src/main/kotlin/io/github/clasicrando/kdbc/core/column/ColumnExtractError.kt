package io.github.clasicrando.kdbc.core.column

import io.github.clasicrando.kdbc.core.exceptions.KdbcException
import kotlin.reflect.KType

class ColumnExtractError(type: KType, value: Any)
    : KdbcException("Expected column to be of type $type, but got a value of $value")
