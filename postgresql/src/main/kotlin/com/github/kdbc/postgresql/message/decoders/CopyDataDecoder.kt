package com.github.kdbc.postgresql.message.decoders

import com.github.kdbc.core.buffer.ByteReadBuffer
import com.github.kdbc.core.message.MessageDecoder
import com.github.kdbc.core.use
import com.github.kdbc.postgresql.message.PgMessage

/**
 * [MessageDecoder] for [PgMessage.CopyData]. This message is sent from the backend during a `COPY
 * TO` and represents a row in the requested format (text or binary). The contents is a [ByteArray]
 * for the data row.
 *
 * [docs](https://www.postgresql.org/docs/current/protocol-message-formats.html#PROTOCOL-MESSAGE-FORMATS-COPYDATA)
 */
internal object CopyDataDecoder : MessageDecoder<PgMessage.CopyData> {
    override fun decode(buffer: ByteReadBuffer): PgMessage.CopyData {
        return PgMessage.CopyData(buffer.use { it.readBytes() })
    }
}
