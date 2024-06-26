package io.github.clasicrando.kdbc.postgresql.message.encoders

import io.github.clasicrando.kdbc.core.buffer.ByteWriteBuffer
import io.github.clasicrando.kdbc.core.buffer.writeLengthPrefixed
import io.github.clasicrando.kdbc.core.message.MessageEncoder
import io.github.clasicrando.kdbc.postgresql.message.PgMessage

/**
 * [MessageEncoder] for [PgMessage.PasswordMessage]. This message is sent to the backend when the
 * authentication flow requires a password only. The contents are:
 * - a header [Byte] of 'p'
 * - the length of the following data (including the size of the [Int] length)
 * - the desired password as a CString (encrypted if requested)
 *
 * [docs](https://www.postgresql.org/docs/current/protocol-message-formats.html#PROTOCOL-MESSAGE-FORMATS-PASSWORDMESSAGE)
 */
internal object PasswordEncoder : MessageEncoder<PgMessage.PasswordMessage> {
    override fun encode(value: PgMessage.PasswordMessage, buffer: ByteWriteBuffer) {
        buffer.writeByte(value.code)
        buffer.writeLengthPrefixed(includeLength = true) {
            writeBytes(value.password)
            writeByte(0)
        }
    }
}
