package com.github.clasicrando.common.buffer

import com.github.clasicrando.common.AutoRelease
import java.nio.charset.Charset

/**
 * Buffer containing a fixed size [ByteArray] where reads against the buffer are always read
 * forward. The data contained within this buffer is not always readable if this instance is a
 * slice over the original buffer. This is done using a size and offset property that are
 * originally set to the [ByteArray.size] property of the backing buffer and 0, respectively. If
 * the instance is constructed using the [slice] method, the new slice's size is the length
 * requested and the offset is calculated current [position] and the pre-slice buffer's offset. The
 * [position] property keeps track of the relative position within the buffer and reads against the
 * buffer increments the [position] value based the number of bytes requested.
 */
class ByteReadBuffer(
    private var innerBuffer: ByteArray,
    private val offset: Int = 0,
    private val size: Int = innerBuffer.size,
) : AutoRelease {
    private var position: Int = 0

    /** Move cursor forward the exact amount specified by [byteCount] */
    fun skip(byteCount: Int) {
        position += byteCount
    }

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
        require(length <= remaining) {
            "Cannot slice a buffer with a length ($length) that is more than the remaining " +
                    "bytes ($remaining)"
        }
        return ByteReadBuffer(innerBuffer, position + offset, length)
    }

    /** Number of bytes remaining as readable within the buffer */
    val remaining: Int get() = size - position

    private fun checkExhausted() {
        if (position >= size || offset + position >= innerBuffer.size) {
            throw BufferExhausted()
        }
    }

    /**
     * Read the next available [Byte] within the buffer.
     *
     * @throws BufferExhausted if the buffer has been exhausted
     */
    fun readByte(): Byte {
        checkExhausted()
        return innerBuffer[offset + position++]
    }

    /**
     * Read the next available [Short] within the buffer (requires 2 bytes).
     *
     * @throws BufferExhausted if the buffer has been exhausted
     */
    fun readShort(): Short {
        val result = (
            this.readByte().toInt() and 0xff shl 8
            or (this.readByte().toInt() and 0xff))
        return result.toShort()
    }

    /**
     * Read the next available [Int] within the buffer (requires 4 bytes).
     *
     * @throws BufferExhausted if the buffer has been exhausted
     */
    fun readInt(): Int {
        val result = (
            (this.readByte().toInt() and 0xff shl 24)
            or (this.readByte().toInt() and 0xff shl 16)
            or (this.readByte().toInt() and 0xff shl 8)
            or (this.readByte().toInt() and 0xff))
        return result
    }

    /**
     * Read the next available [Long] within the buffer (requires 8 bytes).
     *
     * @throws BufferExhausted if the buffer has been exhausted
     */
    fun readLong(): Long {
        val result = (
            (this.readByte().toLong() and 0xffL shl 56)
            or (this.readByte().toLong() and 0xffL shl 48)
            or (this.readByte().toLong() and 0xffL shl 40)
            or (this.readByte().toLong() and 0xffL shl 32)
            or (this.readByte().toLong() and 0xffL shl 24)
            or (this.readByte().toLong() and 0xffL shl 16)
            or (this.readByte().toLong() and 0xffL shl 8)
            or (this.readByte().toLong() and 0xffL))
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
        if (remaining < length) {
            throw BufferExhausted()
        }
        return ByteArray(length) { this.readByte() }
    }

    /** Read all remaining bytes into a [ByteArray]. This can result in an empty array. */
    fun readBytes(): ByteArray {
        return ByteArray(remaining) { this.readByte() }
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
     * Read bytes until 0 is returned from [readByte] indicating the end of a CString (null
     * terminated char array). These bytes are then converted to a [String] using the specified
     * [charset]. By default, the bytes are read using [Charsets.UTF_8].
     *
     * @throws BufferExhausted if the buffer has been exhausted before finding a zero byte
     * @throws java.nio.charset.MalformedInputException error decoding the CString bytes
     */
    fun readCString(charset: Charset = Charsets.UTF_8): String {
        return generateSequence { this@ByteReadBuffer.readByte().takeIf { it != ZERO_BYTE } }
            .toList()
            .toByteArray()
            .toString(charset = charset)
    }

    /**
     * Reset the buffer's position to 0 and set the inner buffer to an empty [ByteArray]. This
     * leaves the buffer in an unusable state
     */
    override fun release() {
        position = 0
        innerBuffer = ByteArray(0)
    }

    companion object {
        const val ZERO_BYTE: Byte = 0
    }
}
