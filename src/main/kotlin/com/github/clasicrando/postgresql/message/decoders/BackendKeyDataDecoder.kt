package com.github.clasicrando.postgresql.message.decoders

import com.github.clasicrando.common.message.MessageDecoder
import com.github.clasicrando.postgresql.message.PgMessage
import io.ktor.utils.io.core.ByteReadPacket
import io.ktor.utils.io.core.readInt

internal object BackendKeyDataDecoder : MessageDecoder<PgMessage.BackendKeyData> {
    override fun decode(packet: ByteReadPacket): PgMessage.BackendKeyData {
        return PgMessage.BackendKeyData(packet.readInt(), packet.readInt())
    }
}
