package com.github.clasicrando.postgresql.message.encoders

import com.github.clasicrando.common.message.MessageEncoder
import com.github.clasicrando.postgresql.message.PgMessage
import io.ktor.utils.io.charsets.Charset
import java.nio.ByteBuffer

internal class DescribeEncoder(private val charset: Charset) : MessageEncoder<PgMessage.Describe> {
    override fun encode(value: PgMessage.Describe, buffer: ByteBuffer) {
        buffer.putCode(value)
        buffer.putLengthPrefixed {
            put(value.target.code)
            putCString(value.name, charset = charset)
        }
    }
}
