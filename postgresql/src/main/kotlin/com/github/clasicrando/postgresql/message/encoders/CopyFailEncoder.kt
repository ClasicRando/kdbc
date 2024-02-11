package com.github.clasicrando.postgresql.message.encoders

import com.github.clasicrando.common.buffer.writeLengthPrefixed
import com.github.clasicrando.common.message.MessageEncoder
import com.github.clasicrando.common.message.MessageSendBuffer
import com.github.clasicrando.postgresql.message.PgMessage

internal object CopyFailEncoder : MessageEncoder<PgMessage.CopyFail> {
    override fun encode(value: PgMessage.CopyFail, buffer: MessageSendBuffer) {
        buffer.writeCode(value)
        buffer.writeLengthPrefixed(includeLength = true) {
            writeCString(value.message)
        }
    }
}
