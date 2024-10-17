package io.github.clasicrando.kdbc.core.type

import io.github.clasicrando.kdbc.core.buffer.ByteWriteBuffer

/**
 * Interface defining a type that decodes values of the input type [T] into an argument buffer.
 * This interface should not be inherited by an instantiable class but rather an object to define
 * encoders only once and reuse the instance since the object should hold no state and [encode] is
 * a pure function.
 */
interface Encode<in T : Any> {
    /** Encode the [value] into the [buffer] as a collection of [Byte]s */
    fun encode(value: T, buffer: ByteWriteBuffer)
}
