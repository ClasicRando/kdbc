package com.github.clasicrando.postgresql.message.decoders

import com.github.clasicrando.common.message.MessageDecoder
import com.github.clasicrando.postgresql.message.PgMessage.Companion.ZERO_CODE
import io.ktor.utils.io.charsets.Charset
import io.ktor.utils.io.core.ByteReadPacket

abstract class InformationResponseDecoder<T>(private val charset: Charset) : MessageDecoder<T> {
    fun decodeToFields(packet: ByteReadPacket): Map<Char, String> {
        return buildMap {
            while (!packet.endOfInput) {
                val kind = packet.readByte()
                if (kind != ZERO_CODE) {
                    put(kind.toInt().toChar(), packet.readCString(charset))
                }
            }
        }
    }
}
