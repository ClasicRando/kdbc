package io.github.clasicrando.kdbc.core.stream

import io.github.clasicrando.kdbc.core.DefaultUniqueResourceId
import io.github.clasicrando.kdbc.core.buffer.ByteReadBuffer
import io.github.clasicrando.kdbc.core.buffer.ByteWriteBuffer
import io.github.clasicrando.kdbc.core.resourceLogger
import io.github.oshai.kotlinlogging.KotlinLogging
import io.github.oshai.kotlinlogging.Level
import kotlinx.coroutines.withTimeout
import java.net.InetSocketAddress
import java.nio.ByteBuffer
import java.nio.channels.AsynchronousSocketChannel
import kotlin.coroutines.Continuation
import kotlin.coroutines.suspendCoroutine
import kotlin.time.Duration

private val logger = KotlinLogging.logger {}

/**
 * [AsyncStream] implementation for an JVM NIO2 [AsynchronousSocketChannel]. Each method is
 * implemented by using the socket's callback API and [suspendCoroutine] to suspend until the
 * callback is executed to resume the coroutine.
 */
class Nio2AsyncStream(
    private val address: InetSocketAddress,
) : AsyncStream, DefaultUniqueResourceId() {
    private val socket = AsynchronousSocketChannel.open()
    private val readBuffer = ByteBuffer.allocateDirect(4096)
        .apply { limit(0) }

    override val isConnected: Boolean get() = socket.isOpen

    override suspend fun connect(timeout: Duration) {
        require(timeout.isPositive()) { "Timeout must be positive" }
        try {
            withTimeout(timeout) {
                suspendCoroutine { continuation: Continuation<Unit> ->
                    socket.connect(
                        address,
                        null,
                        SuspendVoidCompletionHandler(continuation),
                    )
                }
            }
        } catch (ex: Throwable) {
            logger.resourceLogger(this, Level.TRACE) {
                message = "Failed to connect to {address}"
                payload = mapOf("address" to address)
                cause = ex
            }
            throw StreamConnectError(
                io.ktor.network.sockets.InetSocketAddress(address.hostName, address.port),
                ex,
            )
        }
        logger.resourceLogger(this, Level.TRACE) {
            message = "Successfully connected to {address}"
            payload = mapOf("address" to address)
        }
    }

    override suspend fun writeBuffer(buffer: ByteWriteBuffer) {
        val byteBuffer = ByteBuffer.allocate(0)
        while (byteBuffer.hasRemaining()) {
            val bytes = try {
                suspendCoroutine { continuation: Continuation<Int> ->
                    socket.write(byteBuffer, null, SuspendCompletionHandler(continuation))
                }
            } catch (ex: Throwable) {
                logger.resourceLogger(this, Level.TRACE) {
                    message = ""
                    payload = mapOf()
                    cause = ex
                }
                throw StreamWriteError(ex)
            }
            logger.resourceLogger(this, Level.TRACE) {
                message = "Wrote {count} bytes to {address}"
                payload = mapOf("count" to bytes, "address" to address)
            }
        }
    }

    private suspend fun readIntoBuffer(min: Int) {
        readBuffer.clear()
        while (true) {
            val bytesRead = try {
                suspendCoroutine { continuation: Continuation<Int> ->
                    socket.read(readBuffer, null, SuspendCompletionHandler(continuation))
                }
            } catch (ex: Throwable) {
                logger.resourceLogger(this, Level.TRACE) {
                    message = "Failed to read from socket"
                    cause = ex
                }
                throw StreamReadError(ex)
            }
            if (bytesRead == -1) {
                logger.resourceLogger(this, Level.TRACE) {
                    message = "Unexpectedly reached end of stream"
                }
                throw EndOfStream()
            }
            logger.resourceLogger(this, Level.TRACE) {
                message = "Received {count} bytes from {address}"
                payload = mapOf("count" to bytesRead, "address" to address)
            }
            if (readBuffer.position() >= min) {
                readBuffer.flip()
                return
            }
            if (!readBuffer.hasRemaining()) {
                throw FullBuffer()
            }
        }
    }

    override suspend fun readByte(): Byte {
        if (readBuffer.remaining() >= 1) {
            return readBuffer.get()
        }
        readIntoBuffer(1)
        return readBuffer.get()
    }

    override suspend fun readInt(): Int {
        if (readBuffer.remaining() >= 4) {
            return readBuffer.getInt()
        }
        val available = readBuffer.remaining()
        val intRead = ByteBuffer.allocate(4)
        while (readBuffer.hasRemaining()) {
            intRead.put(readBuffer.get())
        }

        readIntoBuffer(4 - available)

        while (intRead.hasRemaining()) {
            intRead.put(readBuffer.get())
        }
        intRead.flip()
        return intRead.getInt()
    }

    override suspend fun readBuffer(count: Int): ByteReadBuffer {
        val bytes = ByteArray(count)
        if (readBuffer.remaining() >= count) {
            readBuffer.get(bytes)
            return ByteReadBuffer(bytes)
        }
        val available = readBuffer.remaining()
        readBuffer.get(bytes, 0, available)

        readIntoBuffer(count - available)

        readBuffer.get(bytes, available, bytes.size - available)
        return ByteReadBuffer(bytes)
    }

    override fun release() {
        socket.close()
    }
}
