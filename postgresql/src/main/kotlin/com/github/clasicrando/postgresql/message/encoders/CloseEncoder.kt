package com.github.clasicrando.postgresql.message.encoders

import com.github.clasicrando.common.buffer.writeLengthPrefixed
import com.github.clasicrando.common.message.MessageEncoder
import com.github.clasicrando.common.message.MessageSendBuffer
import com.github.clasicrando.postgresql.message.PgMessage

internal object CloseEncoder : MessageEncoder<PgMessage.Close> {
    override fun encode(value: PgMessage.Close, buffer: MessageSendBuffer) {
        buffer.writeCode(value)
        buffer.writeLengthPrefixed(includeLength = true) {
            writeByte(value.target.code)
            writeCString(value.targetName ?: "")
        }
    }
}
