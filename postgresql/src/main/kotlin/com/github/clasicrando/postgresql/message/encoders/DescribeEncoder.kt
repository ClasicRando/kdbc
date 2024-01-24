package com.github.clasicrando.postgresql.message.encoders

import com.github.clasicrando.common.message.MessageEncoder
import com.github.clasicrando.postgresql.message.PgMessage
import io.ktor.utils.io.charsets.Charset
import io.ktor.utils.io.core.BytePacketBuilder

internal class DescribeEncoder(private val charset: Charset) : MessageEncoder<PgMessage.Describe> {
    override fun encode(value: PgMessage.Describe, buffer: BytePacketBuilder) {
        buffer.writeCode(value)
        buffer.writeLengthPrefixed {
            writeByte(value.target.code)
            writeCString(value.name, charset = charset)
        }
    }
}
