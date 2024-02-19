package com.github.clasicrando.postgresql.message.encoders

import com.github.clasicrando.common.buffer.writeFully
import com.github.clasicrando.common.message.MessageEncoder
import com.github.clasicrando.common.message.MessageSendBuffer
import com.github.clasicrando.postgresql.message.PgMessage

internal object PasswordEncoder : MessageEncoder<PgMessage.PasswordMessage> {
    override fun encode(value: PgMessage.PasswordMessage, buffer: MessageSendBuffer) {
        buffer.writeCode(value)
        buffer.writeLengthPrefixed(includeLength = true) {
            writeFully(value.password)
            writeByte(0)
        }
    }
}
