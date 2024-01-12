package com.github.clasicrando.common.message

import io.ktor.utils.io.core.ByteReadPacket

/** Server message decoder. Enables parsing of message bytes into the message type [T] */
internal interface MessageDecoder<out T> {
    /** Parse the [packet] provided into the required output message type [T] */
    fun decode(packet: ByteReadPacket): T
}
