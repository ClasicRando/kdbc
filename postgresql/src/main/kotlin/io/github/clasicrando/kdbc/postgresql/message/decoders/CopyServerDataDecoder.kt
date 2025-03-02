package io.github.clasicrando.kdbc.postgresql.message.decoders

import io.github.clasicrando.kdbc.core.buffer.ByteReadBuffer
import io.github.clasicrando.kdbc.core.message.MessageDecoder
import io.github.clasicrando.kdbc.postgresql.message.PgMessage

/**
 * [MessageDecoder] for [PgMessage.CopyServerData]. This message is sent from the backend during a
 * `COPY TO` and represents a row in the requested format (text or binary). The contents is a
 * [ByteReadBuffer] for the data row.
 *
 * [docs](https://www.postgresql.org/docs/current/protocol-message-formats.html#PROTOCOL-MESSAGE-FORMATS-COPYDATA)
 */
internal object CopyServerDataDecoder : PgMessageDecoder<PgMessage.CopyServerData>() {
    override fun decode(buffer: ByteReadBuffer): PgMessage.CopyServerData {
        return PgMessage.CopyServerData(buffer)
    }
}
