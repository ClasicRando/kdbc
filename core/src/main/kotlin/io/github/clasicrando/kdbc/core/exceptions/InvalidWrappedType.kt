package io.github.clasicrando.kdbc.core.exceptions

import kotlin.reflect.KClass

/**
 * [KdbcException] thrown when an operation to insert column data into a value class instance fails
 */
class InvalidWrappedType(value: Any?, cls: KClass<*>)
    : KdbcException("Could not convert, $value, into ${cls.simpleName}")
