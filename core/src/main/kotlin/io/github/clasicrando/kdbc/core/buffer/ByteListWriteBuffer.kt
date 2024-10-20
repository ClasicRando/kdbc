package io.github.clasicrando.kdbc.core.buffer

/**
 * Buffer that only allows for writing to the inner buffer of a fixed capacity, where the capacity
 * is specified in the constructor but defaults to 2048 (2MB). The recommendation with this kind of
 * write buffer is for a single IO object instance (e.g. socket) you will reuse this buffer for
 * each write since the underling resource is reset when [reset] is called rather than cleaned
 * up.
 */
class ByteListWriteBuffer : ByteWriteBuffer {
    @PublishedApi
    internal var innerBuffer = ArrayList<Byte>()

    /**
     * Position within the internal buffer. This signifies how many bytes have been written to the
     * buffer
     */
    override val position: Int get() = innerBuffer.size

    override val remaining: Int get() = Int.MAX_VALUE - innerBuffer.size

    override fun set(
        position: Int,
        byte: Byte,
    ) {
        require(position < innerBuffer.size) {
            "Cannot write to a location that has not already been written"
        }
        innerBuffer[position] = byte
    }

    /**
     * Write a single [byte] to the buffer
     *
     * @throws BufferOverflow if the buffer does not enough bytes available for write to complete
     * this operation
     */
    override fun writeByte(byte: Byte) {
        innerBuffer.add(byte)
    }

    /**
     * Write a single [short] (2 bytes) to the buffer
     *
     * @throws BufferOverflow if the buffer does not enough bytes available for write to complete
     * this operation
     */
    override fun writeShort(short: Short) {
        innerBuffer.add((short.toInt() ushr 8 and 0xff).toByte())
        innerBuffer.add((short.toInt() and 0xff).toByte())
    }

    /**
     * Write a single [int] (4 bytes) to the buffer
     *
     * @throws BufferOverflow if the buffer does not enough bytes available for write to complete
     * this operation
     */
    override fun writeInt(int: Int) {
        innerBuffer.add((int ushr 24 and 0xff).toByte())
        innerBuffer.add((int ushr 16 and 0xff).toByte())
        innerBuffer.add((int ushr 8 and 0xff).toByte())
        innerBuffer.add((int and 0xff).toByte())
    }

    /**
     * Write a single [long] (8 bytes) to the buffer
     *
     * @throws BufferOverflow if the buffer does not enough bytes available for write to complete
     * this operation
     */
    override fun writeLong(long: Long) {
        innerBuffer.add((long ushr 56 and 0xffL).toByte())
        innerBuffer.add((long ushr 48 and 0xffL).toByte())
        innerBuffer.add((long ushr 40 and 0xffL).toByte())
        innerBuffer.add((long ushr 32 and 0xffL).toByte())
        innerBuffer.add((long ushr 24 and 0xffL).toByte())
        innerBuffer.add((long ushr 16 and 0xffL).toByte())
        innerBuffer.add((long ushr 8 and 0xffL).toByte())
        innerBuffer.add((long and 0xffL).toByte())
    }

    /**
     * Write the contents of the supplied [byteArray], starting at index [offset], writing a total
     * of [length] bytes into the buffer.
     *
     * @throws IllegalArgumentException if the [byteArray] does not have enough contents to satisfy
     * the [offset] and [length] parameters
     * @throws BufferOverflow if the buffer does not enough bytes available for write to complete
     * this operation
     */
    override fun writeBytes(
        byteArray: ByteArray,
        offset: Int,
        length: Int,
    ) {
        require(byteArray.size >= offset + length) {
            "The supplied offset = $offset and length = $length is not valid for a ByteArray of " +
                "size = ${byteArray.size}"
        }
        for (i in offset..<(offset + length)) {
            innerBuffer.add(byteArray[i])
        }
    }

    /**
     * Copy the all previously written bytes of this buffer into a [ByteArray]. This leaves the
     * buffer in an initialized state after completing, as if [reset] was called.
     */
    override fun copyToArray(): ByteArray {
        val array = innerBuffer.toByteArray()
        reset()
        return array
    }

    /**
     * Copy contents from another [otherBuffer] into this buffer. For other [ByteListWriteBuffer]
     * instances, all elements are added to this [innerBuffer] using a bulk append operation. For
     * [ByteArrayWriteBuffer] instances, the contents of the list are copied by iterating over the
     * array and appending to this buffer's [innerBuffer].
     *
     * After a successful copy, the [otherBuffer] has [reset] called to free the resources/reset
     * the buffer.
     *
     * @throws BufferOverflow if the buffer does not enough bytes available to complete this
     * operation
     */
    override fun copyFrom(otherBuffer: ByteWriteBuffer) {
        when (otherBuffer) {
            is ByteArrayWriteBuffer -> {
                for (i in 0..<otherBuffer.position) {
                    innerBuffer.add(otherBuffer.innerBuffer[i])
                }
            }
            is ByteListWriteBuffer -> innerBuffer.addAll(otherBuffer.innerBuffer)
        }
        otherBuffer.reset()
    }

    override fun reset() {
        innerBuffer.clear()
    }

    /** Reset the buffer to its initial state allowing the buffer to be reused */
    override fun close() {
        innerBuffer = ArrayList()
    }
}
