package io.github.clasicrando.kdbc.core.buffer

import io.github.clasicrando.kdbc.core.ZERO_BYTE
import java.nio.charset.Charset

/**
 * Buffer containing a fixed size [ByteArray] where reads against the buffer are always read
 * forward. The data contained within this buffer is not always readable if this instance is a slice
 * over the original buffer. This is done using a size and offset property that are originally set
 * to the [ByteArray.size] property of the backing buffer and 0, respectively. If the instance is
 * constructed using the [slice] method, the new slice's size is the length requested and the offset
 * is calculated using the current [position] and the pre-slice buffer's offset. The [position]
 * property keeps track of the relative position within the buffer and reads against the buffer
 * increments the [position] value based the number of bytes requested.
 */
public class ByteReadBuffer(
    private val innerBuffer: ByteArray,
    private val offset: Int = 0,
    @PublishedApi internal val size: Int = innerBuffer.size,
) {
    @PublishedApi internal var position: Int = 0

    /**
     * Create a sub slice of this [ByteReadBuffer], starting at the current position and having a
     * size as the specified [length].
     *
     * This creates a new instance of [ByteReadBuffer] referencing the same underling [ByteArray]
     * with an offset of the relative current cursor position of this [ByteReadBuffer] and a
     * [length] as required.
     *
     * @throws IllegalArgumentException if the [length] is greater than the number of bytes
     *   [remaining] in the buffer
     */
    public fun slice(length: Int): ByteReadBuffer {
        checkRemaining(length)
        val slice = ByteReadBuffer(innerBuffer, position + offset, length)
        position += length
        return slice
    }

    /** Number of bytes remaining as readable within the buffer */
    @Suppress("NOTHING_TO_INLINE")
    public inline fun remaining(): Int {
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
     * View the next available [Byte] within the buffer without consuming the value.
     *
     * @throws BufferExhausted if the buffer has been exhausted
     */
    public fun peekNext(): Byte {
        checkRemaining(1)
        return innerBuffer[offset + position]
    }

    /**
     * Read the next available [Byte] within the buffer.
     *
     * @throws BufferExhausted if the buffer has been exhausted
     */
    public fun readByte(): Byte {
        checkRemaining(1)
        return innerBuffer[offset + position++]
    }

    /**
     * Read the next available [Byte] within the buffer.
     *
     * @throws BufferExhausted if the buffer has been exhausted
     */
    public fun readByteAsInt(): Int {
        return readByte().toInt() and 0xff
    }

    /**
     * Read the next available [Short] within the buffer (requires 2 bytes).
     *
     * @throws BufferExhausted if the buffer has been exhausted
     */
    public fun readShort(): Short {
        checkRemaining(2)
        val result =
            ((innerBuffer[offset + position++].toInt() and 0xff shl 8) or
                (innerBuffer[offset + position++].toInt() and 0xff))
        return result.toShort()
    }

    /**
     * Read the next available [Short] within the buffer (requires 2 bytes) in LittleEndian order.
     *
     * @throws BufferExhausted if the buffer has been exhausted
     */
    public fun readShortLe(): Short {
        checkRemaining(2)
        val result =
            ((innerBuffer[offset + position++].toInt() and 0xff) or
                (innerBuffer[offset + position++].toInt() and 0xff shl 8))
        return result.toShort()
    }

    /**
     * Read the next available [Int] within the buffer (requires 4 bytes).
     *
     * @throws BufferExhausted if the buffer has been exhausted
     */
    public fun readInt(): Int {
        checkRemaining(4)
        val result =
            ((innerBuffer[offset + position++].toInt() and 0xff shl 24) or
                (innerBuffer[offset + position++].toInt() and 0xff shl 16) or
                (innerBuffer[offset + position++].toInt() and 0xff shl 8) or
                (innerBuffer[offset + position++].toInt() and 0xff))
        return result
    }

    /**
     * Read the next available [Int] within the buffer (requires 4 bytes) in LittleEndian order.
     *
     * @throws BufferExhausted if the buffer has been exhausted
     */
    public fun readIntLe(): Int {
        checkRemaining(4)
        val result =
            ((innerBuffer[offset + position++].toInt() and 0xff) or
                (innerBuffer[offset + position++].toInt() and 0xff shl 8) or
                (innerBuffer[offset + position++].toInt() and 0xff shl 16) or
                (innerBuffer[offset + position++].toInt() and 0xff shl 24))
        return result
    }

    /**
     * Read the next available [Long] within the buffer (requires 8 bytes).
     *
     * @throws BufferExhausted if the buffer has been exhausted
     */
    public fun readLong(): Long {
        checkRemaining(8)
        val result =
            ((innerBuffer[offset + position++].toLong() and 0xffL shl 56) or
                (innerBuffer[offset + position++].toLong() and 0xffL shl 48) or
                (innerBuffer[offset + position++].toLong() and 0xffL shl 40) or
                (innerBuffer[offset + position++].toLong() and 0xffL shl 32) or
                (innerBuffer[offset + position++].toLong() and 0xffL shl 24) or
                (innerBuffer[offset + position++].toLong() and 0xffL shl 16) or
                (innerBuffer[offset + position++].toLong() and 0xffL shl 8) or
                (innerBuffer[offset + position++].toLong() and 0xffL))
        return result
    }

    /**
     * Read the next available [Long] within the buffer (requires 8 bytes) in LittleEndian order.
     *
     * @throws BufferExhausted if the buffer has been exhausted
     */
    public fun readLongLe(): Long {
        checkRemaining(8)
        val result =
            ((innerBuffer[offset + position++].toLong() and 0xffL) or
                (innerBuffer[offset + position++].toLong() and 0xffL shl 8) or
                (innerBuffer[offset + position++].toLong() and 0xffL shl 16) or
                (innerBuffer[offset + position++].toLong() and 0xffL shl 24) or
                (innerBuffer[offset + position++].toLong() and 0xffL shl 32) or
                (innerBuffer[offset + position++].toLong() and 0xffL shl 40) or
                (innerBuffer[offset + position++].toLong() and 0xffL shl 48) or
                (innerBuffer[offset + position++].toLong() and 0xffL shl 56))
        return result
    }

    public fun readIntLe(byteCount: Int): Long {
        require(byteCount in 1..8) { "An integer cannot be expressed by 1-8 bytes" }
        val bytes = readBytes(byteCount)
        var result = 0L
        var shiftValue = 0
        for (i in 1..byteCount) {
            result = result or (bytes[i - 1].toLong() and 0xff shl shiftValue)
            shiftValue += 8
        }
        return result
    }

    /**
     * Read the next available [Float] within the buffer (requires 4 bytes).
     *
     * @throws BufferExhausted if the buffer has been exhausted
     */
    public fun readFloat(): Float {
        return Float.fromBits(this.readInt())
    }

    /**
     * Read the next available [Float] within the buffer (requires 4 bytes).
     *
     * @throws BufferExhausted if the buffer has been exhausted
     */
    public fun readFloatLe(): Float {
        return Float.fromBits(this.readIntLe())
    }

    /**
     * Read the next available [Double] within the buffer (requires 8 bytes).
     *
     * @throws BufferExhausted if the buffer has been exhausted
     */
    public fun readDouble(): Double {
        return Double.fromBits(this.readLong())
    }

    /**
     * Read the next available [Double] within the buffer (requires 8 bytes).
     *
     * @throws BufferExhausted if the buffer has been exhausted
     */
    public fun readDoubleLe(): Double {
        return Double.fromBits(this.readLongLe())
    }

    /**
     * Attempt to read an exact number of bytes specified by [length] into a [ByteArray].
     *
     * @throws BufferExhausted if the [remaining] bytes cannot satisfy the required number of bytes
     */
    public fun readBytes(length: Int): ByteArray {
        checkRemaining(length)
        val start = offset + position
        position += length
        return this.innerBuffer.copyOfRange(start, start + length)
    }

    /** Read all remaining bytes into a [ByteArray]. This can result in an empty array. */
    public fun readBytes(): ByteArray {
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
    public fun readText(charset: Charset = Charsets.UTF_8): String {
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
    public fun readCString(charset: Charset = Charsets.UTF_8): String {
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
    public fun reset() {
        position = 0
    }
}
