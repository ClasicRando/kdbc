package com.github.clasicrando.common.column

import kotlin.reflect.KType

class ColumnExtractError(type: KType, value: Any)
    : Exception("Expected column to be of type $type, but got a value of $value")
