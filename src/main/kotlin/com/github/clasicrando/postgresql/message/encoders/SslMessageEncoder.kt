package com.github.clasicrando.postgresql.message.encoders

import com.github.clasicrando.common.message.MessageEncoder
import com.github.clasicrando.postgresql.message.PgMessage
import java.nio.ByteBuffer

internal object SslMessageEncoder : MessageEncoder<PgMessage.SslRequest> {
    override fun encode(value: PgMessage.SslRequest, buffer: ByteBuffer) {
        buffer.putLengthPrefixed {
            putShort(1234)
            putShort(5679)
        }
    }
}
