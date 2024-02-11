package com.github.clasicrando.postgresql.message.encoders

import com.github.clasicrando.common.buffer.writeInt
import com.github.clasicrando.common.buffer.writeLengthPrefixed
import com.github.clasicrando.common.buffer.writeShort
import com.github.clasicrando.common.message.MessageEncoder
import com.github.clasicrando.common.message.MessageSendBuffer
import com.github.clasicrando.postgresql.message.PgMessage

internal object ParseEncoder : MessageEncoder<PgMessage.Parse> {
    override fun encode(value: PgMessage.Parse, buffer: MessageSendBuffer) {
        buffer.writeCode(value)
        buffer.writeLengthPrefixed(includeLength = true) {
            writeCString(value.preparedStatementName)
            writeCString(value.query)
            writeShort(value.parameterTypes.size.toShort())
            for (type in value.parameterTypes) {
                writeInt(type)
            }
        }
    }
}
