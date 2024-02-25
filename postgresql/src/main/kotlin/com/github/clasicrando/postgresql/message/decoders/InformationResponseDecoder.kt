package com.github.clasicrando.postgresql.message.decoders

import com.github.clasicrando.common.buffer.ByteReadBuffer
import com.github.clasicrando.common.message.MessageDecoder
import com.github.clasicrando.common.use

internal abstract class InformationResponseDecoder<T> : MessageDecoder<T> {
    fun decodeToFields(buffer: ByteReadBuffer): Map<Char, String> {
        return buffer.use { buf ->
            buildMap {
                while (buf.remaining > 0) {
                    val kind = buf.readByte()
                    if (kind != ByteReadBuffer.ZERO_BYTE) {
                        put(kind.toInt().toChar(), buf.readCString())
                    }
                }
            }
        }
    }
}
