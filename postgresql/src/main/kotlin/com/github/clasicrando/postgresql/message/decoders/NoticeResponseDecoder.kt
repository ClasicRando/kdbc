package com.github.clasicrando.postgresql.message.decoders

import com.github.clasicrando.postgresql.message.PgMessage
import io.ktor.utils.io.core.ByteReadPacket

internal object NoticeResponseDecoder : InformationResponseDecoder<PgMessage.NoticeResponse>() {
    override fun decode(packet: ByteReadPacket): PgMessage.NoticeResponse {
        val fields = decodeToFields(packet)
        return PgMessage.NoticeResponse(fields)
    }
}
