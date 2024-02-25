package com.github.clasicrando.postgresql.message.encoders

import com.github.clasicrando.common.buffer.ByteWriteBuffer
import com.github.clasicrando.common.message.MessageEncoder
import com.github.clasicrando.postgresql.message.PgMessage

internal object ExecuteEncoder : MessageEncoder<PgMessage.Execute> {
    override fun encode(value: PgMessage.Execute, buffer: ByteWriteBuffer) {
        buffer.writeCode(value)
        buffer.writeLengthPrefixed(includeLength = true) {
            writeCString(value.portalName ?: "")
            writeInt(value.maxRowCount)
        }
    }
}
