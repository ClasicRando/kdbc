package com.github.clasicrando.postgresql.message.encoders

import com.github.clasicrando.common.message.MessageEncoder
import com.github.clasicrando.postgresql.message.PgMessage
import java.nio.ByteBuffer

object CodeOnlyMessageEncoder : MessageEncoder<PgMessage> {
    override fun encode(value: PgMessage, buffer: ByteBuffer) {
        buffer.putCode(value)
        buffer.putInt(4)
    }
}
