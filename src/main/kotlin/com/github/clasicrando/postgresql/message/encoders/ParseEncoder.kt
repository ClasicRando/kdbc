package com.github.clasicrando.postgresql.message.encoders

import com.github.clasicrando.common.column.TypeRegistry
import com.github.clasicrando.common.message.MessageEncoder
import com.github.clasicrando.postgresql.message.PgMessage
import io.ktor.utils.io.charsets.Charset
import java.nio.ByteBuffer

class ParseEncoder(
    private val charset: Charset,
    private val typeRegistry: TypeRegistry,
) : MessageEncoder<PgMessage.Parse> {
    override fun encode(value: PgMessage.Parse, buffer: ByteBuffer) {
        buffer.putCode(value)
        buffer.putLengthPrefixed {
            putCString(value.preparedStatementName, charset)
            putCString(value.query, charset)
            putShort(value.parameters.size.toShort())
            for (param in value.parameters) {
                putInt(typeRegistry.kindOf(param))
            }
        }
    }
}
