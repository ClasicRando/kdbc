package com.github.clasicrando.postgresql.message.encoders

import com.github.clasicrando.common.message.MessageEncoder
import com.github.clasicrando.postgresql.message.PgMessage
import io.ktor.utils.io.charsets.Charset
import java.nio.ByteBuffer

class SaslResponseEncoder(private val charset: Charset) : MessageEncoder<PgMessage.SaslResponse> {
    override fun encode(value: PgMessage.SaslResponse, buffer: ByteBuffer) {
        buffer.putCode(value)
        buffer.putLengthPrefixed {
            put(value.saslData.toByteArray(charset = charset))
        }
    }
}
