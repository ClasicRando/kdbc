package com.github.clasicrando.postgresql.message.decoders

import com.github.clasicrando.common.buffer.ByteReadBuffer
import com.github.clasicrando.common.message.MessageDecoder
import com.github.clasicrando.common.use
import com.github.clasicrando.postgresql.message.PgMessage
import com.github.clasicrando.postgresql.message.TransactionStatus

internal object ReadyForQueryDecoder : MessageDecoder<PgMessage.ReadyForQuery> {
    override fun decode(buffer: ByteReadBuffer): PgMessage.ReadyForQuery {
        val status = buffer.use {
            TransactionStatus.fromByte(it.readByte())
        }
        return PgMessage.ReadyForQuery(status)
    }
}
