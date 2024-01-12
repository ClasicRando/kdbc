package com.github.clasicrando.postgresql.message.decoders

import com.github.clasicrando.common.message.MessageDecoder
import com.github.clasicrando.postgresql.message.PgMessage
import com.github.clasicrando.postgresql.message.TransactionStatus
import io.ktor.utils.io.core.ByteReadPacket

internal object ReadyForQueryDecoder : MessageDecoder<PgMessage.ReadyForQuery> {
    override fun decode(packet: ByteReadPacket): PgMessage.ReadyForQuery {
        val status = TransactionStatus.fromByte(packet.readByte())
        return PgMessage.ReadyForQuery(status)
    }
}
