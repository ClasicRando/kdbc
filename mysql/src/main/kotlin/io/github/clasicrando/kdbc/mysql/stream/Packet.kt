package io.github.clasicrando.kdbc.mysql.stream

import io.github.clasicrando.kdbc.core.buffer.ByteReadBuffer

internal data class ReadPacket(
    val payloadLength: Int,
    val sequenceId: Byte,
    val payload: ByteReadBuffer,
)
