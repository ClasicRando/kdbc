package com.github.clasicrando.common.buffer

import com.github.clasicrando.common.AutoRelease
import com.github.clasicrando.common.stream.AsyncStream
import java.io.OutputStream
import java.nio.ByteBuffer

class ByteWriteBuffer : AutoRelease {
    // TODO remove direct dependency on java with kotlinx.io once the library matures
    @PublishedApi
    internal var innerBuffer = ByteBuffer.allocateDirect(2048)

    fun writeByte(byte: Byte) {
        innerBuffer.put(byte)
    }

    fun writeShort(short: Short) {
        innerBuffer.putShort(short)
    }

    fun writeInt(int: Int) {
        innerBuffer.putInt(int)
    }

    fun writeLong(long: Long) {
        innerBuffer.putLong(long)
    }

    fun writeFloat(float: Float) {
        innerBuffer.putFloat(float)
    }

    fun writeDouble(double: Double) {
        innerBuffer.putDouble(double)
    }

    fun writeFully(byteArray: ByteArray, offset: Int, length: Int) {
        innerBuffer.put(byteArray, offset, length)
    }

    fun writeFully(byteArray: ByteArray) {
        innerBuffer.put(byteArray, 0, byteArray.size)
    }

    fun writeText(content: String) {
        writeFully(content.toByteArray())
    }

    fun writeCString(content: String) {
        writeFully(content.toByteArray(charset = Charsets.UTF_8))
        writeByte(0)
    }

    override fun release() {
        innerBuffer.clear()
    }

    fun writeToArray(): ByteArray {
        innerBuffer.flip()
        val array = ByteArray(innerBuffer.remaining())
        innerBuffer.get(array)
        innerBuffer.clear()
        return array
    }

    suspend fun writeToAsyncStream(stream: AsyncStream) {
        innerBuffer.flip()
        stream.writeBuffer(this)
    }

    inline fun writeLengthPrefixed(
        includeLength: Boolean = false,
        block: ByteWriteBuffer.() -> Unit,
    ) {
        val startIndex = innerBuffer.position()
        innerBuffer.putInt(0)
        this.block()
        val length = innerBuffer.position() - startIndex - if (includeLength) 0 else 4
        innerBuffer.putInt(startIndex, length)
    }

    fun outputStream(): OutputStream = object : OutputStream() {
        override fun write(b: Int) {
            this@ByteWriteBuffer.writeByte(b.toByte())
        }

        override fun write(b: ByteArray, off: Int, len: Int) {
            this@ByteWriteBuffer.writeFully(byteArray = b, offset = off, length = len)
        }

        override fun close() {}
    }
}
