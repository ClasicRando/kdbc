package com.github.clasicrando.postgresql.message.decoders

import com.github.clasicrando.common.buffer.ReadBuffer
import com.github.clasicrando.postgresql.message.PgMessage

internal object NoticeResponseDecoder : InformationResponseDecoder<PgMessage.NoticeResponse>() {
    override fun decode(buffer: ReadBuffer): PgMessage.NoticeResponse {
        val fields = decodeToFields(buffer)
        return PgMessage.NoticeResponse(fields)
    }
}
