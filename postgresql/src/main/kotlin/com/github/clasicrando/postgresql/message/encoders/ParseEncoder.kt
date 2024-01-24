package com.github.clasicrando.postgresql.message.encoders

import com.github.clasicrando.common.message.MessageEncoder
import com.github.clasicrando.postgresql.message.PgMessage
import io.ktor.utils.io.charsets.Charset
import io.ktor.utils.io.core.BytePacketBuilder
import io.ktor.utils.io.core.writeInt
import io.ktor.utils.io.core.writeShort

internal class ParseEncoder(private val charset: Charset) : MessageEncoder<PgMessage.Parse> {
    override fun encode(value: PgMessage.Parse, buffer: BytePacketBuilder) {
        buffer.writeCode(value)
        buffer.writeLengthPrefixed {
            writeCString(value.preparedStatementName, charset)
            writeCString(value.query, charset)
            writeShort(value.parameterTypes.size.toShort())
            for (type in value.parameterTypes) {
                writeInt(type)
            }
        }
    }
}
