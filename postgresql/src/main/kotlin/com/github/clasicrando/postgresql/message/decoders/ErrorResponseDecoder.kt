package com.github.clasicrando.postgresql.message.decoders

import com.github.clasicrando.common.buffer.ByteReadBuffer
import com.github.clasicrando.postgresql.message.PgMessage

internal object ErrorResponseDecoder : InformationResponseDecoder<PgMessage.ErrorResponse>() {
    override fun decode(buffer: ByteReadBuffer): PgMessage.ErrorResponse {
        val fields = decodeToFields(buffer)
        return PgMessage.ErrorResponse(fields)
    }
}
