package com.github.clasicrando.postgresql.message.encoders

import com.github.clasicrando.common.message.MessageEncoder
import com.github.clasicrando.postgresql.message.PgMessage
import io.ktor.utils.io.charsets.Charset
import java.nio.ByteBuffer

class CloseEncoder(private val charset: Charset) : MessageEncoder<PgMessage.Close> {
    override fun encode(value: PgMessage.Close, buffer: ByteBuffer) {
        buffer.putCode(value)
        buffer.putLengthPrefixed {
            put(value.target.code)
            putCString(value.targetName, charset)
        }
    }
}
