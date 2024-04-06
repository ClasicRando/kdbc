package com.github.clasicrando.common.exceptions

import kotlin.reflect.KClass

/**
 * [KdbcException] thrown when a scalar value is extracted but the expected type does not match the
 * actual type
 */
class IncorrectScalarType(value: Any?, cls: KClass<*>)
    : KdbcException("Could not convert $value into ${cls.simpleName}")
