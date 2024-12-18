package io.github.clasicrando.kdbc.core.message

import kotlinx.io.Buffer

/** Server message decoder. Enables parsing of message bytes into the message type [T] */
public interface MessageDecoder<out T, in C> {
    /** Parse the [buffer] provided into the required output message type [T] */
    public fun decode(buffer: Buffer, context: C): T
}
