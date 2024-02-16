package com.github.clasicrando.common.message

import com.github.clasicrando.common.buffer.ReadBuffer

/** Server message decoder. Enables parsing of message bytes into the message type [T] */
interface MessageDecoder<out T> {
    /** Parse the [buffer] provided into the required output message type [T] */
    fun decode(buffer: ReadBuffer): T
}
