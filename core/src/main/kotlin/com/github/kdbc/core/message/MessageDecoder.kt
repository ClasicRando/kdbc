package com.github.kdbc.core.message

import com.github.kdbc.core.buffer.ByteReadBuffer

/** Server message decoder. Enables parsing of message bytes into the message type [T] */
interface MessageDecoder<out T> {
    /** Parse the [buffer] provided into the required output message type [T] */
    fun decode(buffer: ByteReadBuffer): T
}
