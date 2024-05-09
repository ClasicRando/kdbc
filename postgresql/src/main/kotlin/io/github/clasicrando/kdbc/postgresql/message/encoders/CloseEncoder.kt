package io.github.clasicrando.kdbc.postgresql.message.encoders

import io.github.clasicrando.kdbc.core.buffer.ByteWriteBuffer
import io.github.clasicrando.kdbc.core.buffer.writeLengthPrefixed
import io.github.clasicrando.kdbc.core.message.MessageEncoder
import io.github.clasicrando.kdbc.postgresql.message.PgMessage

/**
 * [MessageEncoder] for [PgMessage.Close]. This message is sent to instruct the backend to close a
 * prepared statement or a portal. The contents are:
 * - a header [Byte] of 'C'
 * - the length of the following data (including the size of the [Int] length)
 * - 'S' or 'P' to target a statement or portal respectively
 * - CString as the name of the statement or portal (can be empty to close the unnamed statement or
 * portal)
 *
 * [docs](https://www.postgresql.org/docs/current/protocol-message-formats.html#PROTOCOL-MESSAGE-FORMATS-CLOSE)
 */
internal object CloseEncoder : MessageEncoder<PgMessage.Close> {
    override fun encode(value: PgMessage.Close, buffer: ByteWriteBuffer) {
        buffer.writeCode(value)
        buffer.writeLengthPrefixed(includeLength = true) {
            writeByte(value.target.code)
            writeCString(value.targetName ?: "")
        }
    }
}
