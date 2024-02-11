package com.github.clasicrando.postgresql.message.encoders

import com.github.clasicrando.common.message.MessageEncoder
import com.github.clasicrando.postgresql.message.PgMessage
import io.ktor.utils.io.charsets.Charset
import io.ktor.utils.io.core.BytePacketBuilder

internal class CloseEncoder(private val charset: Charset) : MessageEncoder<PgMessage.Close> {
    override fun encode(value: PgMessage.Close, buffer: BytePacketBuilder) {
        buffer.writeCode(value)
        buffer.writeLengthPrefixed {
            writeByte(value.target.code)
            writeCString(value.targetName ?: "", charset)
        }
    }
}
