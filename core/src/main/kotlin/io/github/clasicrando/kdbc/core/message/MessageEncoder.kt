package io.github.clasicrando.kdbc.core.message

import io.github.clasicrando.kdbc.core.buffer.ByteWriteBuffer

/**
 * Server message encoder. Enables writing a message of type [T] to the output channel that passes
 * messages to the database server.
 */
public interface MessageEncoder<in T, in C> {
    /** Encode the message [value] of type [T] to the [buffer] supplied */
    public fun encode(value: T, buffer: ByteWriteBuffer, context: C)
}
