package com.github.clasicrando.postgresql.message.decoders

import com.github.clasicrando.postgresql.message.PgMessage
import io.ktor.utils.io.charsets.Charset
import io.ktor.utils.io.core.ByteReadPacket

class ErrorResponseDecoder(
    charset: Charset,
) : InformationResponseDecoder<PgMessage.ErrorResponse>(charset) {
    override fun decode(packet: ByteReadPacket): PgMessage.ErrorResponse {
        val fields = decodeToFields(packet)
        return PgMessage.ErrorResponse(fields)
    }
}