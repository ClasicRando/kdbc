package com.github.clasicrando.common.buffer

import io.ktor.utils.io.ByteWriteChannel

abstract class ArrayWriteBuffer : WriteBuffer {
    @PublishedApi
    internal var innerBuffer = ByteArray(8)
    private var position: Int = 0

    private fun grow() {
        val newSize = if (innerBuffer.size <= 1024) {
            innerBuffer.size.shl(1)
        } else {
            (innerBuffer.size * 1.5).toInt()
        }
        check(newSize <= Int.MAX_VALUE) { "ArrayWriteBuffer size cannot exceed Int.MAX_VALUE" }
        innerBuffer = innerBuffer.copyOf(newSize)
    }

    override val bytesWritten: Int get() = position

    override fun writeByte(byte: Byte) {
        if (position == innerBuffer.size) {
            grow()
        }
        innerBuffer[position++] = byte
    }

    override fun toByteArray(): ByteArray {
        return innerBuffer.copyOf(position)
    }

    override fun release() {
        innerBuffer = ByteArray(8)
        position = 0
    }

    suspend fun copyToByteWriteChannel(byteWriteChannel: ByteWriteChannel) {
        var index = 0
        while (index < position) {
            byteWriteChannel.writeByte(innerBuffer[index])
            index++
        }
    }
}

inline fun <B : ArrayWriteBuffer> B.writeLengthPrefixed(
    includeLength: Boolean = false,
    block: B.() -> Unit,
) {
    val startIndex = this.bytesWritten
    writeByte(0)
    writeByte(0)
    writeByte(0)
    writeByte(0)
    this.block()
    val length = this.bytesWritten - startIndex - if (includeLength) 0 else 4
    innerBuffer[startIndex] = (length ushr 24 and 0xff).toByte()
    innerBuffer[startIndex + 1] = (length ushr 16 and 0xff).toByte()
    innerBuffer[startIndex + 2] = (length ushr 8 and 0xff).toByte()
    innerBuffer[startIndex + 3] = (length and 0xff).toByte()
}
