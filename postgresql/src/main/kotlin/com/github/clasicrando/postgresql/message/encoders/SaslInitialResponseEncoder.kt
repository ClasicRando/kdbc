package com.github.clasicrando.postgresql.message.encoders

import com.github.clasicrando.common.message.MessageEncoder
import com.github.clasicrando.postgresql.message.PgMessage
import io.ktor.utils.io.charsets.Charset
import io.ktor.utils.io.core.BytePacketBuilder
import io.ktor.utils.io.core.writeFully
import io.ktor.utils.io.core.writeInt

internal class SaslInitialResponseEncoder(
    private val charset: Charset,
) : MessageEncoder<PgMessage.SaslInitialResponse> {
    override fun encode(value: PgMessage.SaslInitialResponse, buffer: BytePacketBuilder) {
        buffer.writeCode(value)
        buffer.writeLengthPrefixed {
            writeCString(value.mechanism, charset)
            writeInt(value.saslData.length)
            writeFully(value.saslData.toByteArray(charset = charset))
        }
    }
}
