package com.github.clasicrando.postgresql.message.encoders

import com.github.clasicrando.common.buffer.ByteWriteBuffer
import com.github.clasicrando.common.message.MessageEncoder
import com.github.clasicrando.postgresql.message.PgMessage

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
        buffer.writeCode(value)
        buffer.writeLengthPrefixed(includeLength = true) {
            writeBytes(value.password)
            writeByte(0)
        }
    }
}
