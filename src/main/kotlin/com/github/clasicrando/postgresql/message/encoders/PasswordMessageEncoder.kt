package com.github.clasicrando.postgresql.message.encoders

import com.github.clasicrando.common.message.MessageEncoder
import com.github.clasicrando.postgresql.message.PgMessage
import java.nio.ByteBuffer

object PasswordMessageEncoder : MessageEncoder<PgMessage.PasswordMessage> {
    override fun encode(value: PgMessage.PasswordMessage, buffer: ByteBuffer) {
        buffer.putCode(value)
        buffer.putLengthPrefixed {
            put(value.password)
            put(0)
        }
    }
}
