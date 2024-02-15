package com.github.clasicrando.common.buffer

import io.ktor.utils.io.core.ByteReadPacket
import io.ktor.utils.io.core.readBytes
import java.io.InputStream
import java.nio.charset.Charset

interface ReadBuffer {
    val remaining: Long
    fun readByte(): Byte
    fun release()
}

fun ByteReadPacket.asReadBuffer(): ArrayListReadBuffer = object : ArrayListReadBuffer(this.readBytes()) {}

fun ReadBuffer.readShort(): Short {
    val result = this.readByte().toInt() and 0xff shl 8 or (this.readByte().toInt() and 0xff)
    return result.toShort()
}

fun ReadBuffer.readInt(): Int {
    val result = (
        (this.readByte().toInt() and 0xff shl 24)
        or (this.readByte().toInt() and 0xff shl 16)
        or (this.readByte().toInt() and 0xff shl 8)
        or (this.readByte().toInt() and 0xff))
    return result
}

fun ReadBuffer.readLong(): Long {
    val result = (
        (this.readByte().toLong() and 0xffL shl 56)
        or (this.readByte().toLong() and 0xffL shl 48)
        or (this.readByte().toLong() and 0xffL shl 40)
        or (this.readByte().toLong() and 0xffL shl 32)
        or (this.readByte().toLong() and 0xffL shl 24)
        or (this.readByte().toLong() and 0xffL shl 16)
        or (this.readByte().toLong() and 0xffL shl 8)
        or (this.readByte().toLong() and 0xffL)
    )
    return result
}

fun ReadBuffer.readFloat(): Float {
    return Float.fromBits(this.readInt())
}

fun ReadBuffer.readDouble(): Double {
    return Double.fromBits(this.readLong())
}

fun ReadBuffer.readBytes(length: Int): ByteArray {
    require(remaining >= length) {
        "Cannot read $length bytes since there are only $remaining remaining in the buffer"
    }
    return ByteArray(length) { this.readByte() }
}

fun ReadBuffer.readFully(): ByteArray {
    return ByteArray(remaining.toInt()) { this.readByte() }
}

fun ReadBuffer.readText(charset: Charset = Charsets.UTF_8): String {
    return String(this.readFully(), charset = charset)
}

fun ReadBuffer.inputStream(): InputStream = object : InputStream() {
    override fun close() {}

    override fun read(): Int {
        return this@inputStream.readByte().toInt()
    }
}
