package com.github.clasicrando.postgresql.message.decoders

import com.github.clasicrando.common.message.MessageDecoder
import com.github.clasicrando.postgresql.copy.CopyFormat
import com.github.clasicrando.postgresql.message.PgMessage
import io.ktor.utils.io.core.ByteReadPacket
import io.ktor.utils.io.core.readShort

object CopyOutResponseDecoder : MessageDecoder<PgMessage.CopyOutResponse> {
    override fun decode(packet: ByteReadPacket): PgMessage.CopyOutResponse {
        val copyFormat = CopyFormat.fromByte(packet.readByte())
        val columnCount = packet.readShort().toInt()
        val columnFormats = Array(columnCount) {
            CopyFormat.fromByte(packet.readByte())
        }

        return PgMessage.CopyOutResponse(copyFormat, columnCount, columnFormats)
    }
}
