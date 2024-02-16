package com.github.clasicrando.postgresql.message.decoders

import com.github.clasicrando.common.buffer.ReadBuffer
import com.github.clasicrando.postgresql.message.PgMessage

internal object ErrorResponseDecoder : InformationResponseDecoder<PgMessage.ErrorResponse>() {
    override fun decode(buffer: ReadBuffer): PgMessage.ErrorResponse {
        val fields = decodeToFields(buffer)
        return PgMessage.ErrorResponse(fields)
    }
}
