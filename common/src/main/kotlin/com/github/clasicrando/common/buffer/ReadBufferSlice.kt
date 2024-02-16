package com.github.clasicrando.common.buffer

class ReadBufferSlice(
    private var otherBuffer: ArrayReadBuffer?,
    private var offset: Int,
    private var length: Int,
) : ReadBuffer {
    private var position: Int = offset

    override fun readByte(): Byte {
        return otherBuffer?.get(position++)
            ?: error("Attempted to read byte of release ReadBufferSlice")
    }

    override val remaining: Long get() = (length - (position - offset)).toLong()

    override fun release() {
        otherBuffer = null
        position = 0
        offset = 0
        length = 0
    }
}
