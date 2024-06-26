package io.github.clasicrando.kdbc.core.buffer

import java.nio.charset.Charset

/**
 * Buffer containing a fixed size [ByteArray] where reads against the buffer are always read
 * forward. The data contained within this buffer is not always readable if this instance is a
 * slice over the original buffer. This is done using a size and offset property that are
 * originally set to the [ByteArray.size] property of the backing buffer and 0, respectively. If
 * the instance is constructed using the [slice] method, the new slice's size is the length
 * requested and the offset is calculated using the current [position] and the pre-slice buffer's
 * offset. The [position] property keeps track of the relative position within the buffer and reads
 * against the buffer increments the [position] value based the number of bytes requested.
 */
class ByteReadBuffer(
    private var innerBuffer: ByteArray,
    private val offset: Int = 0,
    @PublishedApi
    internal val size: Int = innerBuffer.size,
) : AutoCloseable {
    @PublishedApi
    internal var position: Int = 0

    /**
     * Create a sub slice of this [ByteReadBuffer], starting at the current position and having a
     * size as the specified [length].
     *
     * This creates a new instance of [ByteReadBuffer] referencing the same underling [ByteArray]
     * with an offset of the relative current cursor position of this [ByteReadBuffer] and a
     * [length] as required.
     *
     * @throws IllegalArgumentException if the [length] is greater than the number of bytes
     * [remaining] in the buffer
     */
    fun slice(length: Int): ByteReadBuffer {
        checkRemaining(length)
        val slice = ByteReadBuffer(innerBuffer, position + offset, length)
        position += length
        return slice
    }

    /** Number of bytes remaining as readable within the buffer */
    @Suppress("NOTHING_TO_INLINE")
    inline fun remaining(): Int {
        return size - position
    }

    /**
     * Check to confirm that the required number of bytes are available within the buffer. If the
     * [remaining] value is not greater than or equal to the [required] byte count,
     * [BufferExhausted] if thrown.
     *
     * @throws [BufferExhausted] if the buffer does not have the required number of bytes available
     */
    private fun checkRemaining(required: Int) {
        if (remaining() < required) {
            throw BufferExhausted(requested = required, remaining = remaining())
        }
    }

    /**
     * Read the next available [Byte] within the buffer.
     *
     * @throws BufferExhausted if the buffer has been exhausted
     */
    fun readByte(): Byte {
        checkRemaining(1)
        return innerBuffer[offset + position++]
    }

    /**
     * Read the next available [Short] within the buffer (requires 2 bytes).
     *
     * @throws BufferExhausted if the buffer has been exhausted
     */
    fun readShort(): Short {
        checkRemaining(2)
        val result = (
            innerBuffer[offset + position++].toInt() and 0xff shl 8
            or (innerBuffer[offset + position++].toInt() and 0xff))
        return result.toShort()
    }

    /**
     * Read the next available [Int] within the buffer (requires 4 bytes).
     *
     * @throws BufferExhausted if the buffer has been exhausted
     */
    fun readInt(): Int {
        checkRemaining(4)
        val result = (
            (innerBuffer[offset + position++].toInt() and 0xff shl 24)
            or (innerBuffer[offset + position++].toInt() and 0xff shl 16)
            or (innerBuffer[offset + position++].toInt() and 0xff shl 8)
            or (innerBuffer[offset + position++].toInt() and 0xff))
        return result
    }

    /**
     * Read the next available [Long] within the buffer (requires 8 bytes).
     *
     * @throws BufferExhausted if the buffer has been exhausted
     */
    fun readLong(): Long {
        checkRemaining(8)
        val result = (
            (innerBuffer[offset + position++].toLong() and 0xffL shl 56)
            or (innerBuffer[offset + position++].toLong() and 0xffL shl 48)
            or (innerBuffer[offset + position++].toLong() and 0xffL shl 40)
            or (innerBuffer[offset + position++].toLong() and 0xffL shl 32)
            or (innerBuffer[offset + position++].toLong() and 0xffL shl 24)
            or (innerBuffer[offset + position++].toLong() and 0xffL shl 16)
            or (innerBuffer[offset + position++].toLong() and 0xffL shl 8)
            or (innerBuffer[offset + position++].toLong() and 0xffL))
        return result
    }

    /**
     * Read the next available [Float] within the buffer (requires 4 bytes).
     *
     * @throws BufferExhausted if the buffer has been exhausted
     */
    fun readFloat(): Float {
        return Float.fromBits(this.readInt())
    }

    /**
     * Read the next available [Double] within the buffer (requires 8 bytes).
     *
     * @throws BufferExhausted if the buffer has been exhausted
     */
    fun readDouble(): Double {
        return Double.fromBits(this.readLong())
    }

    /**
     * Attempt to read an exact number of bytes specified by [length] into a [ByteArray].
     *
     * @throws BufferExhausted if the [remaining] bytes cannot satisfy the required number of bytes
     */
    fun readBytes(length: Int): ByteArray {
        checkRemaining(length)
        val start = offset + position
        position += length
        return this.innerBuffer.copyOfRange(start, start + length)
    }

    /** Read all remaining bytes into a [ByteArray]. This can result in an empty array. */
    fun readBytes(): ByteArray {
        val currentPosition = position
        position = size
        return this.innerBuffer.copyOfRange(offset + currentPosition, offset + size)
    }

    /**
     * Read all remaining bytes using [readBytes] and decode those bytes using the specified
     * [charset]. By default, the bytes are read using [Charsets.UTF_8].
     *
     * @throws java.nio.charset.MalformedInputException error decoding the String bytes
     */
    fun readText(charset: Charset = Charsets.UTF_8): String {
        return String(this.readBytes(), charset = charset)
    }

    /**
     * Read bytes until 0 is found in the current relative [position] indicating the end of a
     * CString (null terminated char array). These bytes are then converted to a [String] using the
     * specified [charset]. By default, the bytes are read using [Charsets.UTF_8].
     *
     * @throws BufferExhausted if the buffer has been exhausted before finding a zero byte
     * @throws java.nio.charset.MalformedInputException error decoding the CString bytes
     */
    fun readCString(charset: Charset = Charsets.UTF_8): String {
        val buffer = ArrayList<Byte>()

        while (remaining() > 0) {
            val nextByte = innerBuffer[offset + position++]
            if (nextByte == ZERO_BYTE) {
                break
            }

            buffer.add(nextByte)
        }
        return String(bytes = buffer.toByteArray(), charset = charset)
    }

    /** Reset this buffer to it's initial reading position so the value can be read again */
    fun reset() {
        position = 0
    }

    /**
     * Reset the buffer's position to 0 and set the inner buffer to an empty [ByteArray]. This
     * leaves the buffer in an unusable state
     */
    override fun close() {
        reset()
        innerBuffer = ByteArray(0)
    }

    companion object {
        const val ZERO_BYTE: Byte = 0
    }
}
