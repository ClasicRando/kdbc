package com.github.clasicrando.postgresql.message.encoders

import com.github.clasicrando.common.message.MessageEncoder
import com.github.clasicrando.postgresql.message.PgMessage
import io.ktor.utils.io.charsets.Charset
import java.nio.ByteBuffer

class SaslInitialResponseEncoder(
    private val charset: Charset,
) : MessageEncoder<PgMessage.SaslInitialResponse> {
    override fun encode(value: PgMessage.SaslInitialResponse, buffer: ByteBuffer) {
        buffer.putCode(value)
        buffer.putLengthPrefixed {
            putCString(value.mechanism, charset)
            putInt(value.saslData.length)
            put(value.saslData.toByteArray(charset = charset))
        }
    }
}
