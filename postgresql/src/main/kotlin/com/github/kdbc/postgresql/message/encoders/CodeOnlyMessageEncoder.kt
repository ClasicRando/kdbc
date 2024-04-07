package com.github.kdbc.postgresql.message.encoders

import com.github.kdbc.core.buffer.ByteWriteBuffer
import com.github.kdbc.core.message.MessageEncoder
import com.github.kdbc.postgresql.message.PgMessage

/**
 * [MessageEncoder] for any [PgMessage] that does not contain any data but the basic header byte
 * and [Int] size.
 *
 * [docs](https://www.postgresql.org/docs/current/protocol-message-formats.html)
 */
internal object CodeOnlyMessageEncoder : MessageEncoder<PgMessage> {
    override fun encode(value: PgMessage, buffer: ByteWriteBuffer) {
        buffer.writeCode(value)
        buffer.writeInt(4)
    }
}
