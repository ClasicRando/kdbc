package com.github.clasicrando.postgresql.message.encoders

import com.github.clasicrando.common.message.MessageEncoder
import com.github.clasicrando.postgresql.message.PgMessage
import io.ktor.utils.io.charsets.Charset
import java.nio.ByteBuffer

class ExecuteEncoder(private val charset: Charset) : MessageEncoder<PgMessage.Execute> {
    override fun encode(value: PgMessage.Execute, buffer: ByteBuffer) {
        buffer.putCode(value)
        buffer.putLengthPrefixed {
            putCString(value.portalName, charset)
            putInt(value.maxRowCount)
        }
    }
}
