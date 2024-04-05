package com.github.clasicrando.common.stream

import com.github.clasicrando.common.AutoRelease
import com.github.clasicrando.common.UniqueResourceId
import com.github.clasicrando.common.buffer.ByteReadBuffer
import com.github.clasicrando.common.buffer.ByteWriteBuffer
import kotlin.time.Duration

private const val RESOURCE_TYPE = "BlockingStream"

/**
 * Interface describing how an asynchronous stream should operate for database connections. The
 * implementation will depend on the platform and compilation target but each method will suspend
 * during IO operation to yield control of the otherwise blocked thread.
 */
interface BlockingStream : UniqueResourceId, AutoRelease {
    override val resourceType: String get() = RESOURCE_TYPE

    /** Returns true if the stream is still connected to the host */
    val isConnected: Boolean

    /**
     * Connect to the host targeted by this stream. This method initiates a connection to the host
     * and waits until the connection has been established or the [timeout] is exceeded.
     *
     * @throws StreamConnectError if the connect operation fails
     * @throws IllegalArgumentException if the [timeout] is not positive
     */
    fun connect(timeout: Duration)

    /**
     * Write all bytes to the supplied [buffer] into the stream. If the number of bytes in the
     * [buffer] exceeds the maximum number of bytes that can be sent in a single write operation,
     * this method will loop until all bytes have been written.
     *
     * @throws StreamWriteError if the write operation fails
     */
    fun writeBuffer(buffer: ByteWriteBuffer)

    /**
     * Read a single [Byte] from the stream.
     *
     * This returns immediately if the stream has a single [Byte] available for read. Otherwise,
     * it initiates a read for available bytes into the internal buffer, reading and returning the
     * first available [Byte].
     *
     * @throws StreamReadError if the read operation fails
     */
    fun readByte(): Byte

    /**
     * Read an [Int] (4 [Byte]s) from the stream.
     *
     * This returns immediately if the stream has 4 [Byte]s available. Otherwise, it initiates a
     * read for available bytes into the internal buffer until the required number of bytes is
     * available. The bytes are then read and returned.
     *
     * @throws StreamReadError if the read operation fails
     */
    fun readInt(): Int

    /**
     * Read the required number of bytes as [count] into a [ByteReadBuffer] and return that buffer.
     *
     * This returns immediately if the stream has [count] bytes available. Otherwise, it initiates
     * a read for available bytes into the internal buffer until the required number of bytes is
     * available. The bytes are then read into the buffer and returned.
     *
     * @throws StreamReadError if the read operation fails
     */
    fun readBuffer(count: Int): ByteReadBuffer
}