package io.github.clasicrando.kdbc.core.buffer

/**
 * Buffer that only allows for writing to the inner buffer of a fixed capacity, where the capacity
 * is specified in the constructor but defaults to 2048 (2MB). The recommendation with this kind of
 * write buffer is for a single IO object instance (e.g. socket) you will reuse this buffer for
 * each write since the underling resource is reset when [release] is called rather than cleaned
 * up.
 */
class ByteArrayWriteBuffer(capacity: Int) : ByteWriteBuffer {
    @PublishedApi
    internal var innerBuffer = ByteArray(capacity)

    /**
     * Position within the internal buffer. This signifies how many bytes have been written to the
     * buffer
     */
    override var position: Int = 0
        internal set

    /** The number of bytes available for writing before the buffer is filled */
    override val remaining: Int get() = innerBuffer.size - position

    private fun checkOverflow(requiredSpace: Int) {
        if (remaining < requiredSpace) {
            throw BufferOverflow(requiredSpace, remaining)
        }
    }

    /**
     * Write a single [byte] to the buffer
     *
     * @throws BufferOverflow if the buffer does not enough bytes available for write to complete
     * this operation
     */
    override fun writeByte(byte: Byte) {
        checkOverflow(1)
        innerBuffer[position++] = byte
    }

    override fun set(position: Int, byte: Byte) {
        require(position <= this.position) {
            "Cannot write to a location that has not already been written"
        }
        innerBuffer[position] = byte
    }

    /**
     * Write a single [short] (2 bytes) to the buffer
     *
     * @throws BufferOverflow if the buffer does not enough bytes available for write to complete
     * this operation
     */
    override fun writeShort(short: Short) {
        checkOverflow(2)
        innerBuffer[position++] = (short.toInt() ushr 8 and 0xff).toByte()
        innerBuffer[position++] = (short.toInt() and 0xff).toByte()
    }

    /**
     * Write a single [int] (4 bytes) to the buffer
     *
     * @throws BufferOverflow if the buffer does not enough bytes available for write to complete
     * this operation
     */
    override fun writeInt(int: Int) {
        checkOverflow(4)
        innerBuffer[position++] = (int ushr 24 and 0xff).toByte()
        innerBuffer[position++] = (int ushr 16 and 0xff).toByte()
        innerBuffer[position++] = (int ushr 8 and 0xff).toByte()
        innerBuffer[position++] = (int and 0xff).toByte()
    }

    /**
     * Write a single [long] (8 bytes) to the buffer
     *
     * @throws BufferOverflow if the buffer does not enough bytes available for write to complete
     * this operation
     */
    override fun writeLong(long: Long) {
        checkOverflow(8)
        innerBuffer[position++] = (long ushr 56 and 0xffL).toByte()
        innerBuffer[position++] = (long ushr 48 and 0xffL).toByte()
        innerBuffer[position++] = (long ushr 40 and 0xffL).toByte()
        innerBuffer[position++] = (long ushr 32 and 0xffL).toByte()
        innerBuffer[position++] = (long ushr 24 and 0xffL).toByte()
        innerBuffer[position++] = (long ushr 16 and 0xffL).toByte()
        innerBuffer[position++] = (long ushr 8 and 0xffL).toByte()
        innerBuffer[position++] = (long and 0xffL).toByte()
    }

    /**
     * Write a single [float] (4 bytes) to the buffer
     *
     * @throws BufferOverflow if the buffer does not enough bytes available for write to complete
     * this operation
     */
    override fun writeFloat(float: Float) {
        checkOverflow(4)
        super.writeFloat(float)
    }

    /**
     * Write a single [double] (8 bytes) to the buffer
     *
     * @throws BufferOverflow if the buffer does not enough bytes available for write to complete
     * this operation
     */
    override fun writeDouble(double: Double) {
        checkOverflow(8)
        super.writeDouble(double)
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
    override fun writeBytes(byteArray: ByteArray, offset: Int, length: Int) {
        require(byteArray.size >= offset + length) {
            "The supplied offset = $offset and length = $length is not valid for a ByteArray of " +
                    "size = ${byteArray.size}"
        }
        checkOverflow(length)
        byteArray.copyInto(innerBuffer, position, offset, offset + length)
        position += length
    }

    /** Reset the buffer to its initial state allowing the buffer to be reused */
    override fun release() {
        position = 0
    }

    /**
     * Copy the all previously written bytes of this buffer into a [ByteArray]. This leaves the
     * buffer in an initialized state after completing, as if [release] was called.
     */
    override fun copyToArray(): ByteArray {
        val array = ByteArray(position)
        for (i in array.indices) {
            array[i] = innerBuffer[i]
        }
        release()
        return array
    }

    override fun copyFrom(buffer: ByteWriteBuffer) {
        when (buffer) {
            is ByteArrayWriteBuffer -> {
                checkOverflow(buffer.position)
                for (i in 0..<buffer.position) {
                    innerBuffer[position++] = buffer.innerBuffer[i]
                }
            }
            is ByteListWriteBuffer -> {
                checkOverflow(buffer.position)
                for (i in 0..<buffer.position) {
                    innerBuffer[position++] = buffer.innerBuffer[i]
                }
            }
        }
        buffer.release()
    }
}
