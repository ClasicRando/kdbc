package com.github.clasicrando.postgresql.message.encoders

import com.github.clasicrando.common.buffer.writeLengthPrefixed
import com.github.clasicrando.common.buffer.writeShort
import com.github.clasicrando.common.message.MessageEncoder
import com.github.clasicrando.common.message.MessageSendBuffer
import com.github.clasicrando.postgresql.message.PgMessage

internal object BindEncoder : MessageEncoder<PgMessage.Bind> {
    override fun encode(value: PgMessage.Bind, buffer: MessageSendBuffer) {
        buffer.writeCode(value)
        buffer.writeLengthPrefixed(includeLength = true) {
            writeCString(value.portal ?: "")
            writeCString(value.statementName)
            writeShort(1)
            writeShort(1)
            value.parameters.writeToBuffer(this)
            writeShort(1)
            writeShort(1)
        }
    }
}
