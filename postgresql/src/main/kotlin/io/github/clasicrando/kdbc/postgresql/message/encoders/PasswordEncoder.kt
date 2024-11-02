package io.github.clasicrando.kdbc.postgresql.message.encoders

import io.github.clasicrando.kdbc.core.message.MessageEncoder
import io.github.clasicrando.kdbc.postgresql.buffer.writeLengthPrefixedInt
import io.github.clasicrando.kdbc.postgresql.message.PgMessage
import kotlinx.io.Sink

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
    override fun encode(
        value: PgMessage.PasswordMessage,
        buffer: Sink,
    ) {
        buffer.writeByte(value.code)
        buffer.writeLengthPrefixedInt(includeLength = true) {
            write(value.password)
            writeByte(0)
        }
    }
}
