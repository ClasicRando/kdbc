package com.github.clasicrando.postgresql.message.encoders

import com.github.clasicrando.common.message.MessageEncoder
import com.github.clasicrando.postgresql.message.PgMessage
import io.ktor.utils.io.core.BytePacketBuilder
import io.ktor.utils.io.core.writeInt

internal object CodeOnlyMessageEncoder : MessageEncoder<PgMessage> {
    override fun encode(value: PgMessage, buffer: BytePacketBuilder) {
        buffer.writeCode(value)
        buffer.writeInt(4)
    }
}
