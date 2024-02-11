package com.github.clasicrando.postgresql.message.encoders

import com.github.clasicrando.common.buffer.writeInt
import com.github.clasicrando.common.buffer.writeLengthPrefixed
import com.github.clasicrando.common.message.MessageEncoder
import com.github.clasicrando.common.message.MessageSendBuffer
import com.github.clasicrando.postgresql.message.PgMessage

internal object ExecuteEncoder : MessageEncoder<PgMessage.Execute> {
    override fun encode(value: PgMessage.Execute, buffer: MessageSendBuffer) {
        buffer.writeCode(value)
        buffer.writeLengthPrefixed(includeLength = true) {
            writeCString(value.portalName ?: "")
            writeInt(value.maxRowCount)
        }
    }
}
