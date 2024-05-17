package io.github.clasicrando.kdbc.core.buffer

/**
 * [ByteWriteBuffer] for writing to the inner [ByteArray] buffer of a fixed capacity, where the
 * capacity is specified in the constructor. The recommendation with this kind of [ByteWriteBuffer]
 * is for a single IO object instance (e.g. socket) to reuse this buffer for each write since the
 * underling resource is reset when [release] is called rather than cleaned up.
 */
class ByteArrayWriteBuffer(capacity: Int) : ByteWriteBuffer {
    @PublishedApi
    internal var innerBuffer = ByteArray(capacity)

    override var position: Int = 0
        internal set

    override val remaining: Int get() = innerBuffer.size - position

    @Suppress("NOTHING_TO_INLINE")
    inline fun remaining() = innerBuffer.size - position

    private fun checkOverflow(requiredSpace: Int) {
        if (remaining() < requiredSpace) {
            throw BufferOverflow(requiredSpace, remaining)
        }
    }

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

    override fun writeShort(short: Short) {
        checkOverflow(2)
        innerBuffer[position++] = (short.toInt() ushr 8 and 0xff).toByte()
        innerBuffer[position++] = (short.toInt() and 0xff).toByte()
    }

    override fun writeInt(int: Int) {
        checkOverflow(4)
        innerBuffer[position++] = (int ushr 24 and 0xff).toByte()
        innerBuffer[position++] = (int ushr 16 and 0xff).toByte()
        innerBuffer[position++] = (int ushr 8 and 0xff).toByte()
        innerBuffer[position++] = (int and 0xff).toByte()
    }

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

    override fun writeFloat(float: Float) {
        checkOverflow(4)
        super.writeFloat(float)
    }

    override fun writeDouble(double: Double) {
        checkOverflow(8)
        super.writeDouble(double)
    }

    override fun writeBytes(byteArray: ByteArray, offset: Int, length: Int) {
        checkOverflow(length)
        require(byteArray.size >= offset + length) {
            "The supplied offset = $offset and length = $length is not valid for a ByteArray of " +
                    "size = ${byteArray.size}"
        }
        byteArray.copyInto(innerBuffer, position, offset, offset + length)
        position += length
    }

    /** Reset the buffer to its initial state allowing the buffer to be reused */
    override fun release() {
        position = 0
    }

    override fun copyToArray(): ByteArray {
        val array = ByteArray(position)
        for (i in array.indices) {
            array[i] = innerBuffer[i]
        }
        release()
        return array
    }

    /**
     * Copy contents from another [otherBuffer] into this buffer. For other [ByteArrayWriteBuffer]
     * instances, the elements already written are copied to the [innerBuffer] using
     * [ByteArray.copyInto]. For [ByteListWriteBuffer] instances, the contents of the list are
     * copied by iterating over the list and appending to this buffer's [innerBuffer].
     *
     * After a successful copy, the [otherBuffer] has [release] called to free the resources/reset
     * the buffer.
     *
     * @throws BufferOverflow if the buffer does not enough bytes available to complete this
     * operation
     */
    override fun copyFrom(otherBuffer: ByteWriteBuffer) {
        when (otherBuffer) {
            is ByteArrayWriteBuffer -> {
                checkOverflow(otherBuffer.position)
                otherBuffer.innerBuffer.copyInto(
                    destination = innerBuffer,
                    destinationOffset = position,
                    startIndex = 0,
                    endIndex = otherBuffer.position,
                )
                position += otherBuffer.position
            }
            is ByteListWriteBuffer -> {
                checkOverflow(otherBuffer.position)
                for (byte in otherBuffer.innerBuffer) {
                    innerBuffer[position++] = byte
                }
            }
        }
        otherBuffer.release()
    }
}
