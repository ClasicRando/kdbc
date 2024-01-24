package com.github.clasicrando.postgresql.message.encoders

import com.github.clasicrando.common.message.MessageEncoder
import com.github.clasicrando.postgresql.message.PgMessage
import io.ktor.utils.io.core.BytePacketBuilder
import io.ktor.utils.io.core.writeShort

internal object SslMessageEncoder : MessageEncoder<PgMessage.SslRequest> {
    override fun encode(value: PgMessage.SslRequest, buffer: BytePacketBuilder) {
        buffer.writeLengthPrefixed {
            writeShort(1234)
            writeShort(5679)
        }
    }
}
