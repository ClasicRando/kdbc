package com.github.clasicrando.common.buffer

import io.ktor.utils.io.ByteWriteChannel
import java.nio.ByteBuffer

abstract class ByteWriteBuffer : WriteBuffer {
    // TODO remove direct dependency on java with kotlinx.io once the library matures
    @PublishedApi
    internal var innerBuffer = ByteBuffer.allocateDirect(2048)

    override fun writeByte(byte: Byte) {
        innerBuffer.put(byte)
    }

    override fun writeShort(short: Short) {
        innerBuffer.putShort(short)
    }

    override fun writeInt(int: Int) {
        innerBuffer.putInt(int)
    }

    override fun writeLong(long: Long) {
        innerBuffer.putLong(long)
    }

    override fun writeFully(byteArray: ByteArray, offset: Int, length: Int) {
        innerBuffer.put(byteArray)
    }

    override fun release() {
        innerBuffer.clear()
    }

    suspend fun copyToByteWriteChannel(byteWriteChannel: ByteWriteChannel) {
        innerBuffer.flip()
        byteWriteChannel.writeFully(innerBuffer)
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

    fun writeCString(content: String) {
        writeFully(content.toByteArray(charset = Charsets.UTF_8))
        writeByte(0)
    }
}
