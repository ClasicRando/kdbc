package com.github.clasicrando.postgresql.message.encoders

import com.github.clasicrando.common.message.MessageEncoder
import com.github.clasicrando.postgresql.message.PgMessage
import io.ktor.utils.io.charsets.Charset
import io.ktor.utils.io.core.BytePacketBuilder
import io.ktor.utils.io.core.writeFully
import io.ktor.utils.io.core.writeInt
import io.ktor.utils.io.core.writeShort

internal class BindEncoder(private val charset: Charset) : MessageEncoder<PgMessage.Bind> {
    override fun encode(value: PgMessage.Bind, buffer: BytePacketBuilder) {
        buffer.writeCode(value)
        buffer.writeLengthPrefixed {
            writeCString(value.portal, charset)
            writeCString(value.statementName, charset)
            writeShort(0)
            writeShort(value.parameters.size.toShort())

            for (bytes in value.parameters) {
                if (bytes == null) {
                    writeInt(-1)
                    continue
                }
                writeInt(bytes.size)
                writeFully(bytes)
            }

            writeShort(0)
        }
    }
}
