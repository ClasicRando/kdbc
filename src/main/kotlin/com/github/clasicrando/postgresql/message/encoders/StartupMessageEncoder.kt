package com.github.clasicrando.postgresql.message.encoders

import com.github.clasicrando.common.message.MessageEncoder
import com.github.clasicrando.postgresql.message.PgMessage
import io.ktor.utils.io.charsets.Charset
import java.nio.ByteBuffer

class StartupMessageEncoder(private val charset: Charset) :
    MessageEncoder<PgMessage.StartupMessage> {
    override fun encode(value: PgMessage.StartupMessage, buffer: ByteBuffer) {
        buffer.putLengthPrefixed {
            putShort(3)
            putShort(0)
            for ((k, v) in value.params) {
                writeCString(k, charset)
                writeCString(v, charset)
            }
            put(0)
        }
    }
}