package com.github.clasicrando.postgresql.message.encoders

import com.github.clasicrando.common.buffer.ByteWriteBuffer
import com.github.clasicrando.common.message.MessageEncoder
import com.github.clasicrando.postgresql.message.PgMessage

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
