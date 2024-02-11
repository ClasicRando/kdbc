package com.github.clasicrando.postgresql.message.encoders

import com.github.clasicrando.common.buffer.writeLengthPrefixed
import com.github.clasicrando.common.message.MessageEncoder
import com.github.clasicrando.common.message.MessageSendBuffer
import com.github.clasicrando.postgresql.message.PgMessage

internal object DescribeEncoder : MessageEncoder<PgMessage.Describe> {
    override fun encode(value: PgMessage.Describe, buffer: MessageSendBuffer) {
        buffer.writeCode(value)
        buffer.writeLengthPrefixed(includeLength = true) {
            writeByte(value.target.code)
            writeCString(value.name)
        }
    }
}
