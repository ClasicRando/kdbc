package com.github.clasicrando.postgresql.message.encoders

import com.github.clasicrando.postgresql.message.PgMessage
import io.ktor.utils.io.charsets.Charset
import java.nio.ByteBuffer

internal fun ByteBuffer.putCode(message: PgMessage) {
    put(message.code)
}

fun ByteBuffer.writeCString(content: String, charset: Charset) {
    put(content.toByteArray(charset = charset))
    put(0)
}

fun ByteBuffer.putCString(content: String, charset: Charset) {
    put(content.toByteArray(charset = charset))
    put(0)
}

inline fun ByteBuffer.putLengthPrefixed(block: ByteBuffer.() -> Unit) {
    val sizeIndex = this.position()
    this.putInt(0)
    this.block()
    val size = this.position() - sizeIndex
    this.putInt(sizeIndex, size)
}
