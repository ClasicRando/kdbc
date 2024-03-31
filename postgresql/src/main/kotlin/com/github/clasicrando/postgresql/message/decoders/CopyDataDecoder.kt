package com.github.clasicrando.postgresql.message.decoders

import com.github.clasicrando.common.buffer.ByteReadBuffer
import com.github.clasicrando.common.message.MessageDecoder
import com.github.clasicrando.common.use
import com.github.clasicrando.postgresql.message.PgMessage

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
