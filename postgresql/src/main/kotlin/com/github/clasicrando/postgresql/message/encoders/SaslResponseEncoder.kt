package com.github.clasicrando.postgresql.message.encoders

import com.github.clasicrando.common.buffer.writeFully
import com.github.clasicrando.common.buffer.writeLengthPrefixed
import com.github.clasicrando.common.message.MessageEncoder
import com.github.clasicrando.common.message.MessageSendBuffer
import com.github.clasicrando.postgresql.message.PgMessage

internal object SaslResponseEncoder : MessageEncoder<PgMessage.SaslResponse> {
    override fun encode(value: PgMessage.SaslResponse, buffer: MessageSendBuffer) {
        buffer.writeCode(value)
        buffer.writeLengthPrefixed(includeLength = true) {
            writeFully(value.saslData.toByteArray(charset = Charsets.UTF_8))
        }
    }
}
