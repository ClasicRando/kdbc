package io.github.clasicrando.kdbc.postgresql.message.encoders

import io.github.clasicrando.kdbc.core.message.MessageEncoder
import io.github.clasicrando.kdbc.postgresql.buffer.writeLengthPrefixed
import io.github.clasicrando.kdbc.postgresql.message.PgMessage
import kotlinx.io.Sink

/**
 * [MessageEncoder] for [PgMessage.SaslResponse]. This message is sent to . The contents are:
 * - a header [Byte] of 'p'
 * - the length of the following data (including the size of the [Int] length)
 * - SASL mechanism specific message data as a [ByteArray]
 *
 * [docs](https://www.postgresql.org/docs/current/protocol-message-formats.html#PROTOCOL-MESSAGE-FORMATS-SASLRESPONSE)
 */
internal object SaslResponseEncoder : MessageEncoder<PgMessage.SaslResponse> {
    override fun encode(value: PgMessage.SaslResponse, buffer: Sink) {
        buffer.writeByte(value.code)
        buffer.writeLengthPrefixed(includeLength = true) {
            write(value.saslData.toByteArray(charset = Charsets.UTF_8))
        }
    }
}
