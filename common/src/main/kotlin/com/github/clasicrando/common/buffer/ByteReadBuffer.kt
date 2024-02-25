package com.github.clasicrando.common.buffer

import com.github.clasicrando.common.AutoRelease
import java.io.InputStream
import java.nio.charset.Charset

class ByteReadBuffer(
    private var innerBuffer: ByteArray,
    private val offset: Int = 0,
    private val size: Int = innerBuffer.size,
) : AutoRelease {
    private var position: Int = 0

    fun skip(byteCount: Int) {
        position += byteCount
    }

    fun slice(length: Int): ByteReadBuffer {
        require(length <= remaining) {
            "Cannot slice a buffer with a length ($length) that is more than the remaining bytes ($remaining)"
        }
        val slice = ByteReadBuffer(innerBuffer, position + offset, length)
        return slice
    }

    val remaining: Int get() = size - position

    fun readByte(): Byte {
        return innerBuffer[offset + position++]
    }

    fun readShort(): Short {
        val result = (
            this.readByte().toInt() and 0xff shl 8
            or (this.readByte().toInt() and 0xff))
        return result.toShort()
    }

    fun readInt(): Int {
        val result = (
            (this.readByte().toInt() and 0xff shl 24)
            or (this.readByte().toInt() and 0xff shl 16)
            or (this.readByte().toInt() and 0xff shl 8)
            or (this.readByte().toInt() and 0xff))
        return result
    }

    fun readLong(): Long {
        val result = (
            (this.readByte().toLong() and 0xffL shl 56)
            or (this.readByte().toLong() and 0xffL shl 48)
            or (this.readByte().toLong() and 0xffL shl 40)
            or (this.readByte().toLong() and 0xffL shl 32)
            or (this.readByte().toLong() and 0xffL shl 24)
            or (this.readByte().toLong() and 0xffL shl 16)
            or (this.readByte().toLong() and 0xffL shl 8)
            or (this.readByte().toLong() and 0xffL))
        return result
    }

    fun readFloat(): Float {
        return Float.fromBits(this.readInt())
    }

    fun readDouble(): Double {
        return Double.fromBits(this.readLong())
    }

    fun readBytes(length: Int): ByteArray {
        require(remaining >= length) {
            "Cannot read $length bytes since there are only $remaining remaining in the buffer"
        }
        return ByteArray(length) { this.readByte() }
    }

    fun readBytes(): ByteArray {
        return ByteArray(remaining) { this.readByte() }
    }

    fun readText(charset: Charset = Charsets.UTF_8): String {
        return String(this.readBytes(), charset = charset)
    }

    fun readCString(charset: Charset = Charsets.UTF_8): String {
        val temp = generateSequence {
            val byte = this@ByteReadBuffer.readByte()
            byte.takeIf { it != ZERO_BYTE }
        }.toList().toByteArray()
        return temp.toString(charset = charset)
    }

    fun inputStream(): InputStream = object : InputStream() {
        override fun close() {}

        override fun read(): Int {
            return this@ByteReadBuffer.readByte().toInt()
        }
    }

    override fun release() {
        position = 0
        innerBuffer = ByteArray(0)
    }

    companion object {
        const val ZERO_BYTE: Byte = 0
    }
}
