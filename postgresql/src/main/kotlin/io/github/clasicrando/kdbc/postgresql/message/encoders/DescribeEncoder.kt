package io.github.clasicrando.kdbc.postgresql.message.encoders

import io.github.clasicrando.kdbc.core.buffer.ByteWriteBuffer
import io.github.clasicrando.kdbc.core.buffer.writeLengthPrefixed
import io.github.clasicrando.kdbc.core.message.MessageEncoder
import io.github.clasicrando.kdbc.postgresql.message.PgMessage

/**
 * [MessageEncoder] for [PgMessage.Describe]. This message is sent to ask the backend to describe
 * the specified target as either a statement or portal. The contents are:
 * - a header [Byte] of 'D'
 * - the length of the following data (including the size of the [Int] length)
 * - 'S' or 'P' to target a statement or portal respectively
 * - CString as the name of the statement or portal (can be empty to close the unnamed statement or
 * portal)
 *
 * [docs](https://www.postgresql.org/docs/current/protocol-message-formats.html#PROTOCOL-MESSAGE-FORMATS-DESCRIBE)
 */
internal object DescribeEncoder : MessageEncoder<PgMessage.Describe> {
    override fun encode(value: PgMessage.Describe, buffer: ByteWriteBuffer) {
        buffer.writeByte(value.code)
        buffer.writeLengthPrefixed(includeLength = true) {
            writeByte(value.target.code)
            writeCString(value.name)
        }
    }
}
