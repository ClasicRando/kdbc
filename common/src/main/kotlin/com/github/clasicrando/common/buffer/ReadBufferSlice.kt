package com.github.clasicrando.common.buffer

class ReadBufferSlice private constructor(
    private var otherBuffer: ArrayReadBuffer?,
    private var offset: Int,
    private var length: Int,
) : ReadBuffer {
    internal constructor(otherBuffer: ArrayReadBuffer, length: Int)
            : this(otherBuffer, otherBuffer.position, length)

    private var position: Int = offset

    override fun readByte(): Byte {
        return otherBuffer?.get(position++)
            ?: error("Attempted to read byte of released ReadBufferSlice")
    }

    override val remaining: Long get() = (length - (position - offset)).toLong()

    override fun release() {
        otherBuffer = null
        position = 0
        offset = 0
        length = 0
    }

    fun subSlice(length: Int): ReadBufferSlice {
        val slice = ReadBufferSlice(otherBuffer, position, length)
        position += length
        return slice
    }
}
