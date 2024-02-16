package com.github.clasicrando.common.buffer

abstract class ArrayReadBuffer(bytes: ByteArray) : ReadBuffer {
    private var position: Int = 0
    private var innerBuffer = bytes

    fun skip(byteCount: Int) {
        position += byteCount
    }

    operator fun get(index: Int): Byte {
        return innerBuffer[index]
    }

    override val remaining: Long get() = (innerBuffer.size - position).toLong()

    override fun readByte(): Byte {
        return innerBuffer[position++]
    }

    override fun release() {
        position = 0
        innerBuffer = ByteArray(0)
    }
}
