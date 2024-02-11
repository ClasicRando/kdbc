package com.github.clasicrando.postgresql.message.encoders

import com.github.clasicrando.common.message.MessageEncoder
import com.github.clasicrando.postgresql.message.PgMessage
import io.ktor.utils.io.charsets.Charset
import io.ktor.utils.io.core.BytePacketBuilder
import io.ktor.utils.io.core.writeShort

internal class BindEncoder(private val charset: Charset) : MessageEncoder<PgMessage.Bind> {
    override fun encode(value: PgMessage.Bind, buffer: BytePacketBuilder) {
        buffer.writeCode(value)
        buffer.writeLengthPrefixed {
            writeCString(value.portal ?: "", charset)
            writeCString(value.statementName, charset)
            writeShort(1)
            writeShort(1)
            value.parameters.writeToBuffer(this)
            writeShort(1)
            writeShort(1)
        }
    }
}
