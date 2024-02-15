package com.github.clasicrando.postgresql.message.decoders

import com.github.clasicrando.common.message.MessageDecoder
import com.github.clasicrando.postgresql.message.PgMessage
import io.ktor.utils.io.charsets.Charset
import io.ktor.utils.io.core.ByteReadPacket

internal object ParameterStatusDecoder : MessageDecoder<PgMessage.ParameterStatus> {
    override fun decode(packet: ByteReadPacket): PgMessage.ParameterStatus {
        return PgMessage.ParameterStatus(
            packet.readCString(),
            packet.readCString(),
        )
    }
}
