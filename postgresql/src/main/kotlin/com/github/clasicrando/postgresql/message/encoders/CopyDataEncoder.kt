package com.github.clasicrando.postgresql.message.encoders

import com.github.clasicrando.common.buffer.ByteWriteBuffer
import com.github.clasicrando.common.message.MessageEncoder
import com.github.clasicrando.postgresql.message.PgMessage

internal object CopyDataEncoder : MessageEncoder<PgMessage.CopyData> {
    override fun encode(value: PgMessage.CopyData, buffer: ByteWriteBuffer) {
        buffer.writeCode(value)
        buffer.writeLengthPrefixed(includeLength = true) {
            writeBytes(value.data)
        }
    }
}
