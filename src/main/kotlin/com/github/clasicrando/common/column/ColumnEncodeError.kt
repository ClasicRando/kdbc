package com.github.clasicrando.common.column

import kotlin.reflect.KClass

class ColumnEncodeError(
    type: KClass<*>,
    value: Any,
) : Throwable("Could not encode '$value' as $type")

inline fun <reified T : Any> columnEncodeError(value: Any): Nothing {
    throw ColumnEncodeError(T::class, value)
}
