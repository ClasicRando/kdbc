package io.github.clasicrando.kdbc.postgresql.message.decoders

import io.github.clasicrando.kdbc.core.buffer.ByteReadBuffer
import io.github.clasicrando.kdbc.core.message.MessageDecoder
import io.github.clasicrando.kdbc.core.use
import io.github.clasicrando.kdbc.postgresql.message.PgMessage
import io.github.clasicrando.kdbc.postgresql.message.TransactionStatus

/**
 * [MessageDecoder] for [PgMessage.ReadyForQuery]. This message is sent when backend has finished
 * processing the current query cycle and is ready for more queries. The content is always a single
 * [Byte] that is translated to a [TransactionStatus].
 *
 * [docs](https://www.postgresql.org/docs/current/protocol-message-formats.html#PROTOCOL-MESSAGE-FORMATS-READYFORQUERY)
 */
internal object ReadyForQueryDecoder : MessageDecoder<PgMessage.ReadyForQuery> {
    override fun decode(buffer: ByteReadBuffer): PgMessage.ReadyForQuery {
        val status = buffer.use {
            TransactionStatus.fromByte(it.readByte())
        }
        return PgMessage.ReadyForQuery(status)
    }
}
