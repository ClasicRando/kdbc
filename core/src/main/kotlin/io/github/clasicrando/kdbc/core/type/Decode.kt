package io.github.clasicrando.kdbc.core.type

import io.github.clasicrando.kdbc.core.column.ColumnDecodeError

/**
 * Interface defining a type that decodes [V]s into the required output type [T]. This interface
 * should not be inherited by an instantiable class but rather an object to define decoders only
 * once and reuse instance since the object should hold no state and [decode] is a pure function.
 */
interface Decode<out T : Any, in V : Any> {
    /**
     * Use the data and context within [value] to return a new instance of [T].
     *
     * @throws ColumnDecodeError If the decode operation fails. In all cases, other exceptions
     * should be caught and [ColumnDecodeError] will be thrown instead to give more context as to
     * why the operation failed.
     */
    fun decode(value: V): T
}
