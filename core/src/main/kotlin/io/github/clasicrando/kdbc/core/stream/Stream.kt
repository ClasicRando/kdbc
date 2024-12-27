package io.github.clasicrando.kdbc.core.stream

import io.github.clasicrando.kdbc.core.UniqueResourceId
import io.github.clasicrando.kdbc.core.buffer.ByteReadBuffer
import kotlinx.io.Sink

/**
 * Interface describing how an asynchronous stream should operate for database connections. The
 * implementation will depend on the platform and compilation target but each method will suspend
 * during IO operation to yield control of the otherwise blocked thread.
 */
public interface Stream : UniqueResourceId, AutoCloseable {
    /** Returns true if the stream is still connected to the host */
    public val isConnected: Boolean

    /**
     * Connect to the host targeted by this stream. This method initiates a connection to the host
     * and suspends until the connection has been established or the timeout is exceeded.
     *
     * @throws StreamConnectError if the connect operation fails
     */
    public suspend fun connect()

    /**
     * Upgrade an existing connection to a TLS stream. This performs a TLS handshake using the
     * existing connection and so the user can continue to use this stream for future operations.
     */
    public suspend fun upgradeTls()

    /**
     * Write bytes to the supplied [Sink]. This exposes the socket's write buffer which is flushed
     * after the write operation is complete.
     *
     * @throws StreamWriteError when the write operation fails
     */
    public suspend fun writeTo(block: suspend (Sink) -> Unit)

    /**
     * Read a single [Byte] from the stream.
     *
     * This returns immediately if the stream has a single [Byte] available for read. Otherwise, it
     * suspends to read available bytes into the internal buffer, reading and returning the first
     * available [Byte].
     *
     * @throws StreamReadError if the stream fails to read a byte
     */
    public suspend fun readByte(): Byte

    /**
     * Read an [Int] (4 [Byte]s) from the stream.
     *
     * This returns immediately if the stream has 4 [Byte]s available. Otherwise, it suspends to
     * read available bytes into the internal buffer until the required number of bytes is
     * available. The bytes are then read and returned.
     *
     * @throws StreamReadError if the stream fails to read bytes
     */
    public suspend fun readInt(): Int

    /**
     * Read the required number of bytes as [count] into a [ByteReadBuffer] and return that buffer.
     *
     * This returns immediately if the stream has [count] bytes available. Otherwise, it suspends to
     * read available bytes into the internal buffer until the required number of bytes is
     * available. The bytes are then read into the buffer and returned.
     *
     * @throws StreamReadError if the stream fails to read bytes
     */
    public suspend fun readBuffer(count: Int): ByteReadBuffer
}
