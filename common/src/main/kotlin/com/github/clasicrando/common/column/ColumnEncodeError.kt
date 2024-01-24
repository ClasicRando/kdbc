package com.github.clasicrando.common.column

import kotlin.reflect.KClass

/** Exception thrown when a value cannot be encoded properly by the specified [DbType] encode */
class ColumnEncodeError(
    type: KClass<*>,
    value: Any,
) : Throwable("Could not encode '$value' as $type")

/**
 * Throw a [ColumnEncodeError] where the [value] is the input to encode and [T] is the expected
 * type of the [DbType] used for encoding.
 */
inline fun <reified T : Any> columnEncodeError(value: Any): Nothing {
    throw ColumnEncodeError(T::class, value)
}
