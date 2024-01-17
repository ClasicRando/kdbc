package com.github.clasicrando.postgresql.message.encoders

import com.github.clasicrando.common.message.MessageEncoder
import com.github.clasicrando.postgresql.message.PgMessage
import io.ktor.utils.io.charsets.Charset
import io.ktor.utils.io.core.BytePacketBuilder
import io.ktor.utils.io.core.writeShort

internal class StartupEncoder(private val charset: Charset) :
    MessageEncoder<PgMessage.StartupMessage> {
    override fun encode(value: PgMessage.StartupMessage, buffer: BytePacketBuilder) {
        buffer.writeLengthPrefixed {
            writeShort(3)
            writeShort(0)
            for ((k, v) in value.params) {
                writeCString(k, charset)
                writeCString(v, charset)
            }
            writeByte(0)
        }
    }
}