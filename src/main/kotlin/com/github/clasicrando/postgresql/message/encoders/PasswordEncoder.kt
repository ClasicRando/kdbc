package com.github.clasicrando.postgresql.message.encoders

import com.github.clasicrando.common.message.MessageEncoder
import com.github.clasicrando.postgresql.message.PgMessage
import io.ktor.utils.io.core.BytePacketBuilder
import io.ktor.utils.io.core.writeFully

internal object PasswordEncoder : MessageEncoder<PgMessage.PasswordMessage> {
    override fun encode(value: PgMessage.PasswordMessage, buffer: BytePacketBuilder) {
        buffer.writeCode(value)
        buffer.writeLengthPrefixed {
            writeFully(value.password)
            writeByte(0)
        }
    }
}
