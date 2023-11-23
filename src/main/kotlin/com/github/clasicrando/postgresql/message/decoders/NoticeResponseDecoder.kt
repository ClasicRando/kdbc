package com.github.clasicrando.postgresql.message.decoders

import com.github.clasicrando.postgresql.message.PgMessage
import io.ktor.utils.io.charsets.Charset
import io.ktor.utils.io.core.ByteReadPacket

class NoticeResponseDecoder(
    charset: Charset,
) : InformationResponseDecoder<PgMessage.NoticeResponse>(charset) {
    override fun decode(packet: ByteReadPacket): PgMessage.NoticeResponse {
        val fields = decodeToFields(packet)
        return PgMessage.NoticeResponse(fields)
    }
}