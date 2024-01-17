package com.github.clasicrando.postgresql.message.encoders

import com.github.clasicrando.common.message.MessageEncoder
import com.github.clasicrando.postgresql.message.PgMessage
import io.ktor.utils.io.charsets.Charset
import io.ktor.utils.io.core.BytePacketBuilder
import io.ktor.utils.io.core.writeFully

internal class SaslResponseEncoder(private val charset: Charset) : MessageEncoder<PgMessage.SaslResponse> {
    override fun encode(value: PgMessage.SaslResponse, buffer: BytePacketBuilder) {
        buffer.writeCode(value)
        buffer.writeLengthPrefixed {
            writeFully(value.saslData.toByteArray(charset = charset))
        }
    }
}
