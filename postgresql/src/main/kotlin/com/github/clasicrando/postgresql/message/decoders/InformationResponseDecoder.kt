package com.github.clasicrando.postgresql.message.decoders

import com.github.clasicrando.common.message.MessageDecoder
import com.github.clasicrando.common.zeroByte
import com.github.clasicrando.postgresql.message.PgMessage.Companion.ZERO_CODE
import io.ktor.utils.io.core.ByteReadPacket

internal abstract class InformationResponseDecoder<T> : MessageDecoder<T> {
    fun decodeToFields(packet: ByteReadPacket): Map<Char, String> {
        return buildMap {
            while (!packet.endOfInput) {
                val kind = packet.readByte()
                if (kind != ZERO_CODE) {
                    put(kind.toInt().toChar(), packet.readCString())
                }
            }
        }
    }
}
