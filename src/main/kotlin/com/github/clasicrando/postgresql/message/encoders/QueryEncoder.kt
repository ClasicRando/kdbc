package com.github.clasicrando.postgresql.message.encoders

import com.github.clasicrando.common.message.MessageEncoder
import com.github.clasicrando.postgresql.message.PgMessage
import io.ktor.utils.io.charsets.Charset
import java.nio.ByteBuffer

internal class QueryEncoder(private val charset: Charset) : MessageEncoder<PgMessage.Query> {
    override fun encode(value: PgMessage.Query, buffer: ByteBuffer) {
        buffer.putCode(value)
        buffer.putLengthPrefixed {
            writeCString(value.query, charset)
        }
    }
}
