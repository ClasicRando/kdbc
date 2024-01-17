package com.github.clasicrando.postgresql.message.encoders

import com.github.clasicrando.common.column.TypeRegistry
import com.github.clasicrando.common.message.MessageEncoder
import com.github.clasicrando.postgresql.message.PgMessage
import io.ktor.utils.io.charsets.Charset
import io.ktor.utils.io.core.BytePacketBuilder
import io.ktor.utils.io.core.writeFully
import io.ktor.utils.io.core.writeInt
import io.ktor.utils.io.core.writeShort

internal class BindEncoder(
    private val charset: Charset,
    private val typeRegistry: TypeRegistry,
) : MessageEncoder<PgMessage.Bind> {
    override fun encode(value: PgMessage.Bind, buffer: BytePacketBuilder) {
        buffer.writeCode(value)
        buffer.writeLengthPrefixed {
            writeCString(value.portal, charset)
            writeCString(value.statementName, charset)
            writeShort(0)
            writeShort(value.parameters.size.toShort())

            for (param in value.parameters) {
                if (param == null) {
                    writeInt(-1)
                    continue
                }

                val encodedValue = typeRegistry.encode(param)
                if (encodedValue == null) {
                    writeInt(-1)
                    continue
                }

                val bytes = encodedValue.toByteArray(charset = charset)
                writeInt(bytes.size)
                writeFully(bytes)
            }

            writeShort(0)
        }
    }
}
