package com.github.clasicrando.common.buffer

class ReadBufferSlice private constructor(
    private var buffer: ArrayReadBuffer?,
    private var offset: Int,
    private var length: Int,
) : ReadBuffer {
    internal constructor(otherBuffer: ArrayReadBuffer, length: Int)
            : this(otherBuffer, otherBuffer.position, length)

    private var position: Int = offset

    override fun readByte(): Byte {
        return buffer?.get(position++)
            ?: error("Attempted to read byte of released ReadBufferSlice")
    }

    override val remaining: Long get() = (length - (position - offset)).toLong()

    override fun release() {
        buffer = null
        position = 0
        offset = 0
        length = 0
    }

    fun subSlice(length: Int): ReadBufferSlice {
        val slice = ReadBufferSlice(buffer, position, length)
        position += length
        return slice
    }
}
