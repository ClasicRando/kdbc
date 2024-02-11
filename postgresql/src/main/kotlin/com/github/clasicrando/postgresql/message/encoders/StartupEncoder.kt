package com.github.clasicrando.postgresql.message.encoders

import com.github.clasicrando.common.buffer.writeLengthPrefixed
import com.github.clasicrando.common.buffer.writeShort
import com.github.clasicrando.common.message.MessageEncoder
import com.github.clasicrando.common.message.MessageSendBuffer
import com.github.clasicrando.postgresql.message.PgMessage

internal object StartupEncoder : MessageEncoder<PgMessage.StartupMessage> {
    override fun encode(value: PgMessage.StartupMessage, buffer: MessageSendBuffer) {
        buffer.writeLengthPrefixed(includeLength = true) {
            writeShort(3)
            writeShort(0)
            for ((k, v) in value.params) {
                writeCString(k)
                writeCString(v)
            }
            writeByte(0)
        }
    }
}