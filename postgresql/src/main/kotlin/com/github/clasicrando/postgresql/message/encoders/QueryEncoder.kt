package com.github.clasicrando.postgresql.message.encoders

import com.github.clasicrando.common.message.MessageEncoder
import com.github.clasicrando.postgresql.message.PgMessage
import io.ktor.utils.io.charsets.Charset
import io.ktor.utils.io.core.BytePacketBuilder

internal class QueryEncoder(private val charset: Charset) : MessageEncoder<PgMessage.Query> {
    override fun encode(value: PgMessage.Query, buffer: BytePacketBuilder) {
        buffer.writeCode(value)
        buffer.writeLengthPrefixed {
            writeCString(value.query, charset)
        }
    }
}
