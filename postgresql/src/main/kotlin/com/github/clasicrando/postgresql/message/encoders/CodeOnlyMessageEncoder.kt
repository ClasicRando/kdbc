package com.github.clasicrando.postgresql.message.encoders

import com.github.clasicrando.common.buffer.writeInt
import com.github.clasicrando.common.message.MessageEncoder
import com.github.clasicrando.common.message.MessageSendBuffer
import com.github.clasicrando.postgresql.message.PgMessage

internal object CodeOnlyMessageEncoder : MessageEncoder<PgMessage> {
    override fun encode(value: PgMessage, buffer: MessageSendBuffer) {
        buffer.writeCode(value)
        buffer.writeInt(4)
    }
}
