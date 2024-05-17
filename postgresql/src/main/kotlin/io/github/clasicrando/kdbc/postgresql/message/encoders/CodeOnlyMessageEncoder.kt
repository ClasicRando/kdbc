package io.github.clasicrando.kdbc.postgresql.message.encoders

import io.github.clasicrando.kdbc.core.buffer.ByteWriteBuffer
import io.github.clasicrando.kdbc.core.message.MessageEncoder
import io.github.clasicrando.kdbc.postgresql.message.PgMessage

/**
 * [MessageEncoder] for any [PgMessage] that does not contain any data but the basic header byte
 * and [Int] size.
 *
 * [docs](https://www.postgresql.org/docs/current/protocol-message-formats.html)
 */
internal object CodeOnlyMessageEncoder : MessageEncoder<PgMessage> {
    override fun encode(value: PgMessage, buffer: ByteWriteBuffer) {
        buffer.writeByte(value.code)
        buffer.writeInt(4)
    }
}
