package com.github.clasicrando.postgresql.message.encoders

import com.github.clasicrando.common.message.MessageEncoder
import com.github.clasicrando.postgresql.message.PgMessage
import io.ktor.utils.io.charsets.Charset
import io.ktor.utils.io.core.BytePacketBuilder

internal class CopyFailEncoder(private val charset: Charset) : MessageEncoder<PgMessage.CopyFail> {
    override fun encode(value: PgMessage.CopyFail, buffer: BytePacketBuilder) {
        buffer.writeCode(value)
        buffer.writeLengthPrefixed {
            writeCString(value.message, charset)
        }
    }
}
