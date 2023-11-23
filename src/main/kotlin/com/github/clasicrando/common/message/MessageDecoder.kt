package com.github.clasicrando.common.message

import io.ktor.utils.io.core.ByteReadPacket

interface MessageDecoder<out T> {
    fun decode(packet: ByteReadPacket): T
}
