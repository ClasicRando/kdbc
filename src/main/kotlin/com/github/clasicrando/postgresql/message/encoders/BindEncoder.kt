package com.github.clasicrando.postgresql.message.encoders

import com.github.clasicrando.common.column.TypeRegistry
import com.github.clasicrando.common.message.MessageEncoder
import com.github.clasicrando.postgresql.message.PgMessage
import io.ktor.utils.io.charsets.Charset
import java.nio.ByteBuffer

internal class BindEncoder(
    private val charset: Charset,
    private val typeRegistry: TypeRegistry,
) : MessageEncoder<PgMessage.Bind> {
    override fun encode(value: PgMessage.Bind, buffer: ByteBuffer) {
        buffer.putCode(value)
        buffer.putLengthPrefixed {
            putCString(value.portal, charset)
            putCString(value.statementName, charset)
            putShort(0)
            putShort(value.parameters.size.toShort())

            for (param in value.parameters) {
                if (param == null) {
                    putInt(-1)
                    continue
                }

                val encodedValue = typeRegistry.encode(param)
                if (encodedValue == null) {
                    putInt(-1)
                    continue
                }

                val bytes = encodedValue.toByteArray(charset = charset)
                putInt(bytes.size)
                put(bytes)
            }

            putShort(0)
        }
    }
}
