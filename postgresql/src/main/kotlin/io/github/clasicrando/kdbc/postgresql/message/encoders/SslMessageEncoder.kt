package io.github.clasicrando.kdbc.postgresql.message.encoders

import io.github.clasicrando.kdbc.core.buffer.ByteWriteBuffer
import io.github.clasicrando.kdbc.core.message.MessageEncoder
import io.github.clasicrando.kdbc.postgresql.message.PgMessage

/**
 * [MessageEncoder] for [PgMessage.SslRequest]. This message is sent to request the start of
 * creating an SSL connection with the postgresql database. The contents are:
 * - the length of the following data (including the size of the [Int] length)
 * - the SSL request code (80877103) split into 2 [Short] values of 1234 and 5679
 *
 * [docs](https://www.postgresql.org/docs/current/protocol-message-formats.html#PROTOCOL-MESSAGE-FORMATS-SSLREQUEST)
 */
internal object SslMessageEncoder : MessageEncoder<PgMessage.SslRequest> {
    override fun encode(value: PgMessage.SslRequest, buffer: ByteWriteBuffer) {
        buffer.writeLengthPrefixed(includeLength = true) {
            writeShort(1234)
            writeShort(5679)
        }
    }
}
