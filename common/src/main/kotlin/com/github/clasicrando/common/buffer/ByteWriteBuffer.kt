package com.github.clasicrando.common.buffer

import com.github.clasicrando.common.AutoRelease
import java.io.OutputStream
import java.nio.charset.Charset

/**
 * Buffer that only allows for writing to the inner buffer of a fixed capacity, where the capacity
 * is specified in the constructor but defaults to 2048 (2MB). The recommendation with this kind of
 * write buffer is for a single IO object instance (e.g. socket) you will reuse this buffer for
 * each write since the underling resource is reset when [release] is called rather than cleaned
 * up.
 */
class ByteWriteBuffer(capacity: Int) : AutoRelease {
    @PublishedApi
    internal var innerBuffer = ByteArray(capacity)

    /**
     * Position within the internal buffer. This signifies how many bytes have been written to the
     * buffer
     */
    var position: Int = 0
        internal set

    /** The number of bytes available for writing before the buffer is filled */
    val remaining: Int get() = innerBuffer.size - position

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
    fun writeByte(byte: Byte) {
        checkOverflow(1)
        innerBuffer[position++] = byte
    }

    /**
     * Write a single [short] (2 bytes) to the buffer
     *
     * @throws BufferOverflow if the buffer does not enough bytes available for write to complete
     * this operation
     */
    fun writeShort(short: Short) {
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
    fun writeInt(int: Int) {
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
    fun writeLong(long: Long) {
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
    fun writeFloat(float: Float) {
        checkOverflow(4)
        writeInt(float.toBits())
    }

    /**
     * Write a single [double] (8 bytes) to the buffer
     *
     * @throws BufferOverflow if the buffer does not enough bytes available for write to complete
     * this operation
     */
    fun writeDouble(double: Double) {
        checkOverflow(8)
        writeLong(double.toBits())
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
        for (i in offset..<(offset + length)) {
            innerBuffer[position++] = byteArray[i]
        }
    }

    /**
     * Write the entire contents of the supplied [byteArray] to the buffer
     *
     * @throws BufferOverflow if the buffer does not enough bytes available for write to complete
     * this operation
     */
    fun writeBytes(byteArray: ByteArray)  {
        writeBytes(byteArray = byteArray, offset = 0, length = byteArray.size)
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
        writeBytes(bytes)
        writeByte(0)
    }

    /** Reset the buffer to its initial state allowing the buffer to be reused */
    override fun release() {
        position = 0
    }

    /**
     * Copy the all previously written bytes of this buffer into a [ByteArray]. This leaves the
     * buffer in an initialized state after completing, as if [release] was called.
     */
    fun copyToArray(): ByteArray {
        val array = ByteArray(position)
        for (i in array.indices) {
            array[i] = innerBuffer[i]
        }
        release()
        return array
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
        var startIndex = position
        writeInt(0)
        this.block()
        val length = position - startIndex - if (includeLength) 0 else 4
        innerBuffer[startIndex++] = (length ushr 24 and 0xff).toByte()
        innerBuffer[startIndex++] = (length ushr 16 and 0xff).toByte()
        innerBuffer[startIndex++] = (length ushr 8 and 0xff).toByte()
        innerBuffer[startIndex] = (length and 0xff).toByte()
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
