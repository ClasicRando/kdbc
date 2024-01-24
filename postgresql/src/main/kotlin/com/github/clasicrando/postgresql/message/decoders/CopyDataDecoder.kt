package com.github.clasicrando.postgresql.message.decoders

import com.github.clasicrando.common.message.MessageDecoder
import com.github.clasicrando.postgresql.message.PgMessage
import io.ktor.utils.io.core.ByteReadPacket
import io.ktor.utils.io.core.readBytes

internal object CopyDataDecoder : MessageDecoder<PgMessage.CopyData> {
    override fun decode(packet: ByteReadPacket): PgMessage.CopyData {
        return PgMessage.CopyData(packet.readBytes())
    }
}
