package io.github.clasicrando.kdbc.postgresql.message.encoders

import io.github.clasicrando.kdbc.core.buffer.ByteWriteBuffer
import io.github.clasicrando.kdbc.core.buffer.writeString
import io.github.clasicrando.kdbc.core.message.MessageEncoder
import io.github.clasicrando.kdbc.postgresql.message.PgMessage

/**
 * [MessageEncoder] for [PgMessage.SaslResponse]. This message is sent to . The contents are:
 * - a header [Byte] of 'p'
 * - the length of the following data (including the size of the [Int] length)
 * - SASL mechanism specific message data as a [ByteArray]
 *
 * [docs](https://www.postgresql.org/docs/current/protocol-message-formats.html#PROTOCOL-MESSAGE-FORMATS-SASLRESPONSE)
 */
internal object SaslResponseEncoder : PgMessageEncoder<PgMessage.SaslResponse>() {
    override fun encode(value: PgMessage.SaslResponse, buffer: ByteWriteBuffer) {
        buffer.writeByte(value.code)
        buffer.writeLengthPrefixedAsInt(includeLength = true) { writeString(value.saslData) }
    }
}
