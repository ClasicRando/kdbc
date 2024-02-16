package com.github.clasicrando.common.buffer

abstract class ArrayReadBuffer(bytes: ByteArray) : ReadBuffer {
    var position: Int = 0
        private set
    private var innerBuffer = bytes
    private val slices = mutableListOf<ReadBufferSlice>()

    fun skip(byteCount: Int) {
        position += byteCount
    }

    fun slice(length: Int): ReadBufferSlice {
        val slice = ReadBufferSlice(this, length)
        slices.add(slice)
        return slice
    }

    operator fun get(index: Int): Byte {
        return innerBuffer[index]
    }

    override val remaining: Long get() = (innerBuffer.size - position).toLong()

    override fun readByte(): Byte {
        return innerBuffer[position++]
    }

    override fun release() {
        for (slice in slices) {
            slice.release()
        }
        position = 0
        innerBuffer = ByteArray(0)
    }
}
