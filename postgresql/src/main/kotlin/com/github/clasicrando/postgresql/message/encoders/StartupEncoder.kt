package com.github.clasicrando.postgresql.message.encoders

import com.github.clasicrando.common.buffer.ByteWriteBuffer
import com.github.clasicrando.common.message.MessageEncoder
import com.github.clasicrando.postgresql.message.PgMessage

internal object StartupEncoder : MessageEncoder<PgMessage.StartupMessage> {
    override fun encode(value: PgMessage.StartupMessage, buffer: ByteWriteBuffer) {
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