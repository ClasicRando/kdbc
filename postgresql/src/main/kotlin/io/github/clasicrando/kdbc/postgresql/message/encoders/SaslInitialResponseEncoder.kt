package io.github.clasicrando.kdbc.postgresql.message.encoders

import io.github.clasicrando.kdbc.core.buffer.ByteWriteBuffer
import io.github.clasicrando.kdbc.core.buffer.writeLengthPrefixed
import io.github.clasicrando.kdbc.core.message.MessageEncoder
import io.github.clasicrando.kdbc.postgresql.message.PgMessage

/**
 * [MessageEncoder] for [PgMessage.SaslInitialResponse]. This message is sent to provide the
 * initial response to a series of messages communicating authentication information. The contents
 * are:
 * - a header [Byte] of 'p'
 * - the length of the following data (including the size of the [Int] length)
 * - the name of the SASL authentication mechanism selected as a CString
 * - the length of the SASL authentication mechanism's initial response as an [Int] (-1 if there is
 * no initial response)
 * - the initial response as a [ByteArray]
 *
 * [docs](https://www.postgresql.org/docs/current/protocol-message-formats.html#PROTOCOL-MESSAGE-FORMATS-SASLINITIALRESPONSE)
 */
internal object SaslInitialResponseEncoder : MessageEncoder<PgMessage.SaslInitialResponse> {
    override fun encode(
        value: PgMessage.SaslInitialResponse,
        buffer: ByteWriteBuffer,
    ) {
        buffer.writeByte(value.code)
        buffer.writeLengthPrefixed(includeLength = true) {
            writeCString(value.mechanism)
            writeInt(value.saslData.length)
            writeBytes(value.saslData.toByteArray(charset = Charsets.UTF_8))
        }
    }
}
