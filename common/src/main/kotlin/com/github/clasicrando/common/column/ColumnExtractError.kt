package com.github.clasicrando.common.column

import com.github.clasicrando.common.exceptions.KdbcException
import kotlin.reflect.KType

class ColumnExtractError(type: KType, value: Any)
    : KdbcException("Expected column to be of type $type, but got a value of $value")
