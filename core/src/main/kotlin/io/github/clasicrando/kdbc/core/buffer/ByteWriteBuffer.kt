package io.github.clasicrando.kdbc.core.buffer

import java.nio.charset.Charset

/**
 * Sealed class for [Byte] buffers that are write only, except where copying the contents to a
 * [ByteArray] for usage elsewhere.
 */
sealed interface ByteWriteBuffer : AutoCloseable {
    /**
     * Position within the buffer. This signifies how many bytes have been written to the buffer
     */
    val position: Int

    /**
     * The remaining space in the buffer as the number of bytes that can be written before overflow
     */
    val remaining: Int

    /**
     * Write a single [byte] to the buffer at a random access location. It is only valid to
     * overwrite a location that has already been written.
     *
     * @throws IllegalArgumentException if the [position] is not a position that has already been
     * written
     */
    operator fun set(position: Int, byte: Byte)

    /**
     * Write a single [byte] to the buffer
     *
     * @throws BufferOverflow if the buffer does not enough bytes available to complete this
     * operation
     */
    fun writeByte(byte: Byte)

    /**
     * Write a single [short] (2 bytes) to the buffer
     *
     * @throws BufferOverflow if the buffer does not enough bytes available to complete this
     * operation
     */
    fun writeShort(short: Short)

    /**
     * Write a single [int] (4 bytes) to the buffer
     *
     * @throws BufferOverflow if the buffer does not enough bytes available to complete this
     * operation
     */
    fun writeInt(int: Int)

    /**
     * Write a single [long] (8 bytes) to the buffer
     *
     * @throws BufferOverflow if the buffer does not enough bytes available to complete this
     * operation
     */
    fun writeLong(long: Long)

    /**
     * Write a single [float] (4 bytes) to the buffer
     *
     * @throws BufferOverflow if the buffer does not enough bytes available to complete this
     * operation
     */
    fun writeFloat(float: Float) {
        writeInt(float.toBits())
    }

    /**
     * Write a single [double] (8 bytes) to the buffer
     *
     * @throws BufferOverflow if the buffer does not enough bytes available to complete this
     * operation
     */
    fun writeDouble(double: Double) {
        writeLong(double.toBits())
    }

    /**
     * Write the contents of the supplied [byteArray], starting at index [offset], writing a total
     * of [length] bytes into the buffer.
     *
     * @throws IllegalArgumentException if the [byteArray] does not have enough contents to satisfy
     * the [offset] and [length] parameters
     * @throws BufferOverflow if the buffer does not enough bytes available to complete this
     * operation
     */
    fun writeBytes(byteArray: ByteArray, offset: Int, length: Int)

    /**
     * Write the entire contents of the supplied [byteArray] to the buffer
     *
     * @throws BufferOverflow if the buffer does not enough bytes available to complete this
     * operation
     */
    fun writeBytes(byteArray: ByteArray)  {
        writeBytes(byteArray = byteArray, offset = 0, length = byteArray.size)
    }

    /**
     * Write the [text] to the buffer by encoding the string using [Charsets.UTF_8]
     *
     * @throws BufferOverflow if the buffer does not enough bytes available to complete this
     * operation
     * @throws java.nio.charset.MalformedInputException error encoding the [text] into bytes
     */
    fun writeText(text: String) {
        writeText(text, charset = Charsets.UTF_8)
    }

    /**
     * Write the [text] to the buffer by encoding the string using the specified [charset].
     *
     * @throws BufferOverflow if the buffer does not enough bytes available to complete this
     * operation
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
     * @throws BufferOverflow if the buffer does not enough bytes available to complete this
     * operation
     * @throws java.nio.charset.MalformedInputException error encoding the [text] into bytes
     */
    fun writeCString(text: String, charset: Charset = Charsets.UTF_8) {
        val bytes = text.toByteArray(charset = charset)
        writeBytes(bytes)
        writeByte(0)
    }

    /**
     * Copy the all previously written bytes of this buffer into a [ByteArray]. This leaves the
     * buffer in an initialized state after completing, as if [reset] was called.
     */
    fun copyToArray(): ByteArray

    /**
     * Copy contents from another [otherBuffer] into this buffer.
     *
     * After a successful copy, the [otherBuffer] has [reset] called to free the resources/reset
     * the buffer.
     *
     * @throws BufferOverflow if the buffer does not enough bytes available to complete this
     * operation
     */
    fun copyFrom(otherBuffer: ByteWriteBuffer)

    /** Reset the buffer so all allocated resources within the buffer can be reused */
    fun reset()
}

/**
 * Write to this buffer, keeping track of the number of bytes written within the [block] to prefix
 * the bytes written with the number of bytes written as an [Int]. By default, the number of bytes
 * used to write the length value to the buffer is not included in the length value but this can be
 * overridden by supplying true for [includeLength].
 *
 * This operation works by capturing the pre-write buffer position, writing a placeholder [Int] of
 * 0 to the buffer, executing the [block] to perform the desired write operations, calculating the
 * number of bytes written (excluding the length bytes if [includeLength] is false) and finally
 * updating the previously written [Int] length with the calculated value. As a consequence of not
 * knowing how many bytes may be written there is no way to verify the buffer has the remaining
 * capacity to successfully write all required bytes, meaning the operation is not transactional
 * and will perform write operations until the buffer overflows or the [block] completes. This
 * should be kept in mind if you attempt to dump the buffer contents when encountering an error.
 *
 * @throws BufferOverflow if the buffer does not enough bytes available for write to complete
 * this operation.
 */
inline fun ByteWriteBuffer.writeLengthPrefixed(
    includeLength: Boolean = false,
    block: ByteWriteBuffer.() -> Unit,
) {
    var startIndex = position
    writeInt(0)
    this.block()
    val length = position - startIndex - if (includeLength) 0 else 4
    this[startIndex++] = (length ushr 24 and 0xff).toByte()
    this[startIndex++] = (length ushr 16 and 0xff).toByte()
    this[startIndex++] = (length ushr 8 and 0xff).toByte()
    this[startIndex] = (length and 0xff).toByte()
}
