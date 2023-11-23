package com.github.clasicrando.postgresql.message.encoders

import com.github.clasicrando.common.message.MessageEncoder
import com.github.clasicrando.postgresql.message.PgMessage
import java.nio.ByteBuffer

object SslMessageEncoder : MessageEncoder<PgMessage.SslRequest> {
    override fun encode(value: PgMessage.SslRequest, buffer: ByteBuffer) {
        buffer.putLengthPrefixed {
            putInt(8)
            putShort(1234)
            putShort(5679)
        }
    }
}
