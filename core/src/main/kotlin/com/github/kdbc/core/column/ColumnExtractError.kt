package com.github.kdbc.core.column

import com.github.kdbc.core.exceptions.KdbcException
import kotlin.reflect.KType

class ColumnExtractError(type: KType, value: Any)
    : KdbcException("Expected column to be of type $type, but got a value of $value")
