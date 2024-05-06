package io.github.clasicrando.kdbc.core.column

import io.github.clasicrando.kdbc.core.exceptions.KdbcException
import kotlin.reflect.KClass
import kotlin.reflect.KType

class ColumnExtractError(type: KType, value: Any, cls: KClass<*>)
    : KdbcException("Expected column to be of type $type, but got a value of $value ($cls)")
