package com.github.clasicrando.postgresql.message.encoders

import com.github.clasicrando.common.buffer.ByteWriteBuffer
import com.github.clasicrando.common.message.MessageEncoder
import com.github.clasicrando.postgresql.message.PgMessage

/**
 * [MessageEncoder] for [PgMessage.SaslResponse]. This message is sent to . The contents are:
 * - a header [Byte] of 'p'
 * - the length of the following data (including the size of the [Int] length)
 * - SASL mechanism specific message data as a [ByteArray]
 *
 * [docs](https://www.postgresql.org/docs/current/protocol-message-formats.html#PROTOCOL-MESSAGE-FORMATS-SASLRESPONSE)
 */
internal object SaslResponseEncoder : MessageEncoder<PgMessage.SaslResponse> {
    override fun encode(value: PgMessage.SaslResponse, buffer: ByteWriteBuffer) {
        buffer.writeCode(value)
        buffer.writeLengthPrefixed(includeLength = true) {
            writeBytes(value.saslData.toByteArray(charset = Charsets.UTF_8))
        }
    }
}
