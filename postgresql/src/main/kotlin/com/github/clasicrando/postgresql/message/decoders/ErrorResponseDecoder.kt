package com.github.clasicrando.postgresql.message.decoders

import com.github.clasicrando.postgresql.message.PgMessage
import io.ktor.utils.io.core.ByteReadPacket

internal object ErrorResponseDecoder : InformationResponseDecoder<PgMessage.ErrorResponse>() {
    override fun decode(packet: ByteReadPacket): PgMessage.ErrorResponse {
        val fields = decodeToFields(packet)
        return PgMessage.ErrorResponse(fields)
    }
}
