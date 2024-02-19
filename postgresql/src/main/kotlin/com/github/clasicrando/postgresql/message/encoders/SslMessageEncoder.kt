package com.github.clasicrando.postgresql.message.encoders

import com.github.clasicrando.common.message.MessageEncoder
import com.github.clasicrando.common.message.MessageSendBuffer
import com.github.clasicrando.postgresql.message.PgMessage

internal object SslMessageEncoder : MessageEncoder<PgMessage.SslRequest> {
    override fun encode(value: PgMessage.SslRequest, buffer: MessageSendBuffer) {
        buffer.writeLengthPrefixed(includeLength = true) {
            writeShort(1234)
            writeShort(5679)
        }
    }
}
