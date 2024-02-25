package com.github.clasicrando.postgresql.message.encoders

import com.github.clasicrando.common.buffer.ByteWriteBuffer
import com.github.clasicrando.common.message.MessageEncoder
import com.github.clasicrando.postgresql.message.PgMessage

internal object SaslInitialResponseEncoder : MessageEncoder<PgMessage.SaslInitialResponse> {
    override fun encode(value: PgMessage.SaslInitialResponse, buffer: ByteWriteBuffer) {
        buffer.writeCode(value)
        buffer.writeLengthPrefixed(includeLength = true) {
            writeCString(value.mechanism)
            writeInt(value.saslData.length)
            writeFully(value.saslData.toByteArray(charset = Charsets.UTF_8))
        }
    }
}
