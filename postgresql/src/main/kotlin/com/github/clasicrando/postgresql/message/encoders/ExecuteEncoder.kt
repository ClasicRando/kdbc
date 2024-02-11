package com.github.clasicrando.postgresql.message.encoders

import com.github.clasicrando.common.message.MessageEncoder
import com.github.clasicrando.postgresql.message.PgMessage
import io.ktor.utils.io.charsets.Charset
import io.ktor.utils.io.core.BytePacketBuilder
import io.ktor.utils.io.core.writeInt

internal class ExecuteEncoder(private val charset: Charset) : MessageEncoder<PgMessage.Execute> {
    override fun encode(value: PgMessage.Execute, buffer: BytePacketBuilder) {
        buffer.writeCode(value)
        buffer.writeLengthPrefixed {
            writeCString(value.portalName ?: "", charset)
            writeInt(value.maxRowCount)
        }
    }
}
