package com.github.clasicrando.postgresql.message.encoders

import com.github.clasicrando.common.message.MessageEncoder
import com.github.clasicrando.postgresql.message.PgMessage
import io.ktor.utils.io.charsets.Charset
import java.nio.ByteBuffer

class CopyFailEncoder(private val charset: Charset) : MessageEncoder<PgMessage.CopyFail> {
    override fun encode(value: PgMessage.CopyFail, buffer: ByteBuffer) {
        buffer.putCode(value)
        buffer.putLengthPrefixed {
            putCString(value.message, charset)
        }
    }
}
