package io.github.clasicrando.kdbc.core.stream

import io.github.clasicrando.kdbc.core.DefaultUniqueResourceId
import io.github.clasicrando.kdbc.core.buffer.ByteReadBuffer
import io.github.clasicrando.kdbc.core.buffer.ByteWriteBuffer
import io.github.clasicrando.kdbc.core.resourceLogger
import io.github.oshai.kotlinlogging.KotlinLogging
import io.github.oshai.kotlinlogging.Level
import io.ktor.network.selector.SelectorManager
import io.ktor.network.sockets.Connection
import io.ktor.network.sockets.Socket
import io.ktor.network.sockets.SocketAddress
import io.ktor.network.sockets.aSocket
import io.ktor.network.sockets.connection
import io.ktor.network.sockets.isClosed
import io.ktor.util.cio.writer
import io.ktor.utils.io.ByteReadChannel
import io.ktor.utils.io.ByteWriteChannel
import io.ktor.utils.io.readAvailable
import io.ktor.utils.io.writeFully
import kotlinx.coroutines.isActive
import kotlinx.coroutines.withTimeout
import kotlinx.io.Buffer
import kotlinx.io.readTo
import kotlin.time.Duration

private val logger = KotlinLogging.logger {}

class KtorAsyncStream(
    private val address: SocketAddress,
    private val selectorManager: SelectorManager,
) : AsyncStream, DefaultUniqueResourceId() {
    private lateinit var connection: Connection
    private val socket: Socket get() = connection.socket
    private val writeChannel: ByteWriteChannel get() = connection.output
    private val readChannel: ByteReadChannel get() = connection.input
    private var buffer = Buffer()

    override val isConnected: Boolean get() = this::connection.isInitialized
            && socket.isActive && !socket.isClosed

    override suspend fun connect(timeout: Duration) {
        require(timeout.isPositive()) { "Timeout must be positive" }
        try {
            connection = withTimeout(timeout) {
                aSocket(selectorManager).tcp().connect(address).connection()
            }
        } catch (ex: Throwable) {
            logger.resourceLogger(this, Level.TRACE) {
                message = "Failed to connect to {address}"
                payload = mapOf("address" to address)
                cause = ex
            }
            throw StreamConnectError(address, ex)
        }
        logger.resourceLogger(this, Level.TRACE) {
            message = "Successfully connected to {address}"
            payload = mapOf("address" to address)
        }
    }

    override suspend fun writeBuffer(buffer: ByteWriteBuffer) {
        check(isConnected) { "Cannot write to a stream that is not connected" }
        val bytes = buffer.copyToArray()
        writeChannel.writeFully(bytes)
        writeChannel.flush()
    }

    private suspend fun readIntoBuffer(required: Long) {
        val tempBuffer = ByteArray(2048)
        while (true) {
            val bytesRead = try {
                readChannel.readAvailable(tempBuffer)
            } catch (ex: Throwable) {
                logger.resourceLogger(this, Level.TRACE) {
                    message = "Failed to read from socket"
                    cause = ex
                }
                throw StreamReadError(ex)
            }
            buffer.write(tempBuffer, 0, bytesRead)
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
            if (buffer.size >= required) {
                return
            }
        }
    }

    override suspend fun readByte(): Byte {
        check(isConnected) { "Cannot read from a stream that is not connected" }
        if (buffer.size >= 1L) {
            return buffer.readByte()
        }
        readIntoBuffer(1)
        return buffer.readByte()
    }

    override suspend fun readInt(): Int {
        check(isConnected) { "Cannot read from a stream that is not connected" }
        if (buffer.size >= 4) {
            return buffer.readInt()
        }
        readIntoBuffer(4)
        return buffer.readInt()
    }

    override suspend fun readBuffer(count: Int): ByteReadBuffer {
        check(isConnected) { "Cannot read from a stream that is not connected" }
        val destination = ByteArray(count)

        if (buffer.size >= count) {
            buffer.readTo(destination)
            return ByteReadBuffer(destination)
        }
        readIntoBuffer(count.toLong())
        buffer.readTo(destination)
        return ByteReadBuffer(destination)
    }

    override fun release() {
        socket.close()
    }
}
