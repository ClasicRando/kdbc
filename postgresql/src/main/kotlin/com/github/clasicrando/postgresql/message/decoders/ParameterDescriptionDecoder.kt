package com.github.clasicrando.postgresql.message.decoders

import com.github.clasicrando.common.message.MessageDecoder
import com.github.clasicrando.postgresql.message.PgMessage
import io.ktor.utils.io.core.ByteReadPacket
import io.ktor.utils.io.core.readInt
import io.ktor.utils.io.core.readShort

internal object ParameterDescriptionDecoder : MessageDecoder<PgMessage.ParameterDescription> {
    override fun decode(packet: ByteReadPacket): PgMessage.ParameterDescription {
        val parameterCount = packet.readShort()
        val parameterTypes = (1..parameterCount).map { packet.readInt() }
        return PgMessage.ParameterDescription(parameterCount, parameterTypes)
    }
}
