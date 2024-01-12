package com.github.clasicrando.postgresql.message.encoders

import com.github.clasicrando.common.message.MessageEncoder
import com.github.clasicrando.postgresql.message.PgMessage
import java.nio.ByteBuffer

internal object CopyDataEncoder : MessageEncoder<PgMessage.CopyData> {
    override fun encode(value: PgMessage.CopyData, buffer: ByteBuffer) {
        buffer.putCode(value)
        buffer.putLengthPrefixed {
            put(value.data)
        }
    }
}
