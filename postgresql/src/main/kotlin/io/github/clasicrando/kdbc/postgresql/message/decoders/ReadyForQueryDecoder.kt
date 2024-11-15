package io.github.clasicrando.kdbc.postgresql.message.decoders

import io.github.clasicrando.kdbc.core.message.MessageDecoder
import io.github.clasicrando.kdbc.postgresql.message.PgMessage
import io.github.clasicrando.kdbc.postgresql.message.TransactionStatus
import kotlinx.io.Source

/**
 * [MessageDecoder] for [PgMessage.ReadyForQuery]. This message is sent when backend has finished
 * processing the current query cycle and is ready for more queries. The content is always a single
 * [Byte] that is translated to a [TransactionStatus].
 *
 * [docs](https://www.postgresql.org/docs/current/protocol-message-formats.html#PROTOCOL-MESSAGE-FORMATS-READYFORQUERY)
 */
internal object ReadyForQueryDecoder : PgMessageDecoder<PgMessage.ReadyForQuery>() {
    override fun decode(buffer: Source): PgMessage.ReadyForQuery {
        val status = TransactionStatus.fromByte(buffer.readByte())
        return PgMessage.ReadyForQuery(status)
    }
}
