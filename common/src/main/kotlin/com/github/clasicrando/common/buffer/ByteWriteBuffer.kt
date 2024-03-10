package com.github.clasicrando.common.buffer

import com.github.clasicrando.common.AutoRelease
import com.github.clasicrando.common.stream.AsyncStream
import java.io.OutputStream
import java.nio.ByteBuffer
import java.nio.charset.Charset

/**
 * Buffer that only allows for writing to the inner buffer of a fixed capacity, where the capacity
 * is specified in the constructor but defaults to 2048 (2MB). The recommendation with this kind of
 * write buffer is for a single IO object instance (e.g. socket) you will reuse this buffer for
 * each write since the underling resource is reset when [release] is called rather than cleaned
 * up.
 */
class ByteWriteBuffer(capacity: Int = 2048, isDirect: Boolean = true) : AutoRelease {
    // TODO remove direct dependency on java with kotlinx.io once the library matures
    @PublishedApi
    internal var innerBuffer = if (isDirect) {
        ByteBuffer.allocateDirect(capacity)
    } else {
        ByteBuffer.allocate(capacity)
    }

    /**
     * Position within the internal buffer. This signifies how many bytes have been written to the
     * buffer
     */
    val position: Int get() = innerBuffer.position()

    /** The number of bytes available for writing before the buffer is filled */
    val remaining: Int get() = innerBuffer.remaining()

    private fun checkOverflow(requiredSpace: Int) {
        if (innerBuffer.remaining() < requiredSpace) {
            throw BufferOverflow(requiredSpace, innerBuffer.remaining())
        }
    }

    /**
     * Write a single [byte] to the buffer
     *
     * @throws BufferOverflow if the buffer does not enough bytes available for write to complete
     * this operation
     */
    fun writeByte(byte: Byte) {
        checkOverflow(1)
        innerBuffer.put(byte)
    }

    /**
     * Write a single [short] (2 bytes) to the buffer
     *
     * @throws BufferOverflow if the buffer does not enough bytes available for write to complete
     * this operation
     */
    fun writeShort(short: Short) {
        checkOverflow(2)
        innerBuffer.putShort(short)
    }

    /**
     * Write a single [int] (4 bytes) to the buffer
     *
     * @throws BufferOverflow if the buffer does not enough bytes available for write to complete
     * this operation
     */
    fun writeInt(int: Int) {
        checkOverflow(4)
        innerBuffer.putInt(int)
    }

    /**
     * Write a single [long] (8 bytes) to the buffer
     *
     * @throws BufferOverflow if the buffer does not enough bytes available for write to complete
     * this operation
     */
    fun writeLong(long: Long) {
        checkOverflow(8)
        innerBuffer.putLong(long)
    }

    /**
     * Write a single [float] (4 bytes) to the buffer
     *
     * @throws BufferOverflow if the buffer does not enough bytes available for write to complete
     * this operation
     */
    fun writeFloat(float: Float) {
        checkOverflow(4)
        innerBuffer.putFloat(float)
    }

    /**
     * Write a single [double] (8 bytes) to the buffer
     *
     * @throws BufferOverflow if the buffer does not enough bytes available for write to complete
     * this operation
     */
    fun writeDouble(double: Double) {
        checkOverflow(8)
        innerBuffer.putDouble(double)
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
    fun writeBytes(byteArray: ByteArray, offset: Int, length: Int) {
        require(byteArray.size >= offset + length) {
            "The supplied offset = $offset and length = $length is not valid for a ByteArray of " +
                    "size = ${byteArray.size}"
        }
        checkOverflow(length)
        innerBuffer.put(byteArray, offset, length)
    }

    /**
     * Write the entire contents of the supplied [byteArray] to the buffer
     *
     * @throws BufferOverflow if the buffer does not enough bytes available for write to complete
     * this operation
     */
    fun writeBytes(byteArray: ByteArray) {
        checkOverflow(byteArray.size)
        innerBuffer.put(byteArray, 0, byteArray.size)
    }

    /**
     * Write the [text] to the buffer by encoding the string using [Charsets.UTF_8]
     *
     * @throws BufferOverflow if the buffer does not enough bytes available for write to complete
     * this operation
     * @throws java.nio.charset.MalformedInputException error encoding the [text] into bytes
     */
    fun writeText(text: String) {
        writeText(text, charset = Charsets.UTF_8)
    }

    /**
     * Write the [text] to the buffer by encoding the string using the specified [charset].
     *
     * @throws BufferOverflow if the buffer does not enough bytes available for write to complete
     * this operation
     * @throws java.nio.charset.MalformedInputException error encoding the [text] into bytes
     */
    fun writeText(text: String, charset: Charset) {
        writeBytes(text.toByteArray(charset = charset))
    }

    /**
     * Write the [text] to the buffer by encoding the string using the specified [charset] and
     * finishing with a null terminator (zero byte). By default, the [text] is written using
     * [Charsets.UTF_8].
     *
     * @throws BufferOverflow if the buffer does not enough bytes available for write to complete
     * this operation
     * @throws java.nio.charset.MalformedInputException error encoding the [text] into bytes
     */
    fun writeCString(text: String, charset: Charset = Charsets.UTF_8) {
        val bytes = text.toByteArray(charset = charset)
        val requiredSize = bytes.size + 1
        checkOverflow(requiredSize)
        innerBuffer.put(bytes)
        innerBuffer.put(0)
    }

    /** Reset the buffer to its initial state allowing the buffer to be reused */
    override fun release() {
        innerBuffer.clear()
    }

    /**
     * Copy the all previously written bytes of this buffer into a [ByteArray]. This leaves the
     * buffer in an initialized state after completing, as if [release] was called.
     */
    fun writeToArray(): ByteArray {
        innerBuffer.flip()
        val array = ByteArray(innerBuffer.remaining())
        innerBuffer.get(array)
        innerBuffer.clear()
        return array
    }

    /**
     * Write all previously written bytes of this buffer into the [stream] using
     * [AsyncStream.writeBuffer]. After successfully writing all available bytes into the [stream]
     * the buffer is reset as if [release] was called.
     */
    suspend fun writeToAsyncStream(stream: AsyncStream) {
        innerBuffer.flip()
        stream.writeBuffer(this)
        innerBuffer.clear()
    }

    /**
     * Write to this buffer, keeping track of the number of bytes written within the [block] to
     * prefix the bytes written with the number of bytes written as an [Int]. By default, the
     * number of bytes used to write the length value to the buffer is not included in the length
     * value but this can be overridden by supplying true for [includeLength].
     *
     * This operation works by capturing the pre-write buffer position, writing a placeholder [Int]
     * of 0 to the buffer, executing the [block] to perform the desired write operations,
     * calculating the number of bytes written (excluding the length bytes if [includeLength] is
     * false) and finally updating the previously written [Int] length with the calculated value.
     * As a consequence of not knowing how many bytes may be written there is no way to verify the
     * buffer has the remaining capacity to successfully write all required bytes, meaning the
     * operation is not transactional and will perform write operations until the buffer
     * overflows or the [block] completes. This should be kept in mind if you attempt to dump the
     * buffer contents when encountering an error.
     *
     * @throws BufferOverflow if the buffer does not enough bytes available for write to complete
     * this operation.
     */
    inline fun writeLengthPrefixed(
        includeLength: Boolean = false,
        block: ByteWriteBuffer.() -> Unit,
    ) {
        check(innerBuffer.remaining() >= 4) {
            "Cannot write the length value because the buffer does not have 4 bytes available"
        }
        val startIndex = innerBuffer.position()
        innerBuffer.putInt(0)
        this.block()
        val length = innerBuffer.position() - startIndex - if (includeLength) 0 else 4
        innerBuffer.putInt(startIndex, length)
    }

    /**
     * Create a wrapper [OutputStream] for this buffer. Calls the [writeByte] method when the
     * [OutputStream.write] method is called. Calls the [writeBytes] method when
     * [OutputStream.write] is called.
     */
    fun outputStream(): OutputStream = object : OutputStream() {
        override fun write(b: Int) {
            this@ByteWriteBuffer.writeByte(b.toByte())
        }

        override fun write(b: ByteArray, off: Int, len: Int) {
            this@ByteWriteBuffer.writeBytes(byteArray = b, offset = off, length = len)
        }

        override fun close() {}
    }
}
