package com.github.clasicrando.common.stream

import com.github.clasicrando.common.buffer.ByteReadBuffer
import com.github.clasicrando.common.buffer.ByteWriteBuffer
import io.github.oshai.kotlinlogging.KotlinLogging
import java.net.InetSocketAddress
import java.nio.ByteBuffer
import java.nio.channels.AsynchronousSocketChannel
import kotlin.coroutines.suspendCoroutine

private val logger = KotlinLogging.logger {}

/**
 * [AsyncStream] implementation for an JVM NIO2 [AsynchronousSocketChannel]. Each method is
 * implemented by using the socket's callback API and [suspendCoroutine] to suspend until the
 * callback is executed to resume the coroutine.
 */
class Nio2AsyncStream(private val address: InetSocketAddress) : AsyncStream {
    private val socket = AsynchronousSocketChannel.open()
    private val readBuffer = ByteBuffer.allocateDirect(4096)
        .apply { limit(0) }

    override val isConnected: Boolean get() = socket.isOpen

    override suspend fun connect() {
        suspendCoroutine { continuation ->
            socket.connect(
                address,
                null,
                SuspendVoidCompletionHandler(continuation),
            )
        }
        logger.atTrace {
            message = "Successfully connected to {address}"
            payload = mapOf("address" to address)
        }
    }

    override suspend fun writeBuffer(buffer: ByteWriteBuffer) {
        while (buffer.innerBuffer.hasRemaining()) {
            val bytes = suspendCoroutine { continuation ->
                socket.write(
                    buffer.innerBuffer,
                    null,
                    SuspendCompletionHandler(continuation),
                )
            }
            logger.atTrace {
                message = "Wrote {count} bytes to {address}"
                payload = mapOf("count" to bytes, "address" to address)
            }
        }
    }

    private suspend fun readIntoBuffer(min: Int) {
        readBuffer.clear()
        while (true) {
            val bytesRead = suspendCoroutine { continuation ->
                socket.read(readBuffer, null, SuspendCompletionHandler(continuation))
            }
            if (bytesRead == -1) {
                logger.trace { "Unexpectedly reached end of stream" }
                throw EndOfStream()
            }
            logger.atTrace {
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
