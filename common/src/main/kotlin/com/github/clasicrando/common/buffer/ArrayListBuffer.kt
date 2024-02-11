package com.github.clasicrando.common.buffer

abstract class ArrayListBuffer : Buffer {
    @PublishedApi
    internal val innerBuffer = ArrayList<Byte>()

    override fun writeByte(byte: Byte) {
        innerBuffer.add(byte)
    }

    override fun toByteArray(): ByteArray {
        return innerBuffer.toByteArray()
    }
}

inline fun <B : ArrayListBuffer> B.writeLengthPrefixed(block: (B) -> Unit) {
    val startIndex = innerBuffer.size
    writeInt(0)
    block(this)
    val currentSize = innerBuffer.size - startIndex - 4
    innerBuffer[startIndex] = (currentSize ushr 24 and 0xff).toByte()
    innerBuffer[startIndex + 1] = (currentSize ushr 16 and 0xff).toByte()
    innerBuffer[startIndex + 2] = (currentSize ushr 8 and 0xff).toByte()
    innerBuffer[startIndex + 3] = (currentSize and 0xff).toByte()
}
