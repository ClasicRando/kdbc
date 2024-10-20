package io.github.clasicrando.kdbc.core.stream

import io.github.clasicrando.kdbc.core.DefaultUniqueResourceId
import io.github.clasicrando.kdbc.core.buffer.ByteReadBuffer
import io.github.clasicrando.kdbc.core.buffer.ByteWriteBuffer
import io.github.clasicrando.kdbc.core.config.Kdbc
import io.github.clasicrando.kdbc.core.logWithResource
import io.github.oshai.kotlinlogging.KotlinLogging
import io.ktor.network.selector.SelectorManager
import io.ktor.network.sockets.Connection
import io.ktor.network.sockets.Socket
import io.ktor.network.sockets.SocketAddress
import io.ktor.network.sockets.aSocket
import io.ktor.network.sockets.connection
import io.ktor.network.sockets.isClosed
import io.ktor.network.tls.tls
import io.ktor.utils.io.ByteReadChannel
import io.ktor.utils.io.ByteWriteChannel
import io.ktor.utils.io.readAvailable
import io.ktor.utils.io.writeFully
import kotlinx.coroutines.TimeoutCancellationException
import kotlinx.coroutines.isActive
import kotlinx.coroutines.withTimeout
import kotlinx.io.Buffer
import kotlinx.io.readTo
import kotlin.time.Duration

private val logger = KotlinLogging.logger {}

class KtorStream(
    private val address: SocketAddress,
    private val selectorManager: SelectorManager,
) : Stream, DefaultUniqueResourceId() {
    private lateinit var connection: Connection
    private lateinit var socket: Socket
    private lateinit var writeChannel: ByteWriteChannel
    private lateinit var readChannel: ByteReadChannel
    private val buffer = Buffer()
    private val tempBuffer = ByteArray(DEFAULT_BUFFER_SIZE)

    override val isConnected: Boolean get() =
        this::connection.isInitialized &&
            socket.isActive && !socket.isClosed

    override suspend fun connect(timeout: Duration) {
        require(timeout.isPositive()) { "Timeout must be positive" }
        try {
            connection =
                withTimeout(timeout) {
                    aSocket(selectorManager).tcp().connect(address).connection()
                }
            socket = connection.socket
            writeChannel = connection.output
            readChannel = connection.input
        } catch (ex: Exception) {
            logWithResource(logger, Kdbc.detailedLogging) {
                message = "Failed to connect to $address"
                cause = ex
            }
            throw StreamConnectError(address, ex)
        }
        logWithResource(logger, Kdbc.detailedLogging) {
            message = "Successfully connected to $address"
        }
    }

    override suspend fun upgradeTls(timeout: Duration) {
        connection =
            withTimeout(timeout) {
                connection.tls(coroutineContext = selectorManager.coroutineContext)
                    .connection()
            }
        socket = connection.socket
        writeChannel = connection.output
        readChannel = connection.input
    }

    override suspend fun writeBuffer(buffer: ByteWriteBuffer) {
        check(isConnected) { "Cannot write to a stream that is not connected" }
        val bytes = buffer.copyToArray()
        writeChannel.writeFully(bytes)
        writeChannel.flush()
    }

    private suspend fun readIntoBuffer(required: Long) {
        var bytesRequired = required
        while (true) {
            val bytesRead =
                try {
                    readChannel.readAvailable(tempBuffer)
                } catch (ex: TimeoutCancellationException) {
                    throw ex
                } catch (ex: Exception) {
                    logWithResource(logger, Kdbc.detailedLogging) {
                        message = "Failed to read from socket"
                        cause = ex
                    }
                    throw StreamReadError(ex)
                }

            if (bytesRead == -1) {
                logWithResource(logger, Kdbc.detailedLogging) {
                    message = "Unexpectedly reached end of stream"
                }
                throw EndOfStream()
            }
            logWithResource(logger, Kdbc.detailedLogging) {
                message = "Received $bytesRead bytes from $address"
            }

            buffer.write(tempBuffer, 0, bytesRead)
            bytesRequired -= bytesRead
            if (bytesRequired <= 0) {
                return
            }
        }
    }

    override suspend fun readByte(): Byte {
        check(isConnected) { "Cannot read from a stream that is not connected" }
        if (buffer.size < 1L) {
            readIntoBuffer(1)
        }
        return buffer.readByte()
    }

    override suspend fun readInt(): Int {
        check(isConnected) { "Cannot read from a stream that is not connected" }
        if (buffer.size < 4) {
            readIntoBuffer(4 - buffer.size)
        }
        return buffer.readInt()
    }

    override suspend fun readBuffer(count: Int): ByteReadBuffer {
        check(isConnected) { "Cannot read from a stream that is not connected" }
        val destination = ByteArray(count)

        if (buffer.size < count) {
            readIntoBuffer(count - buffer.size)
        }
        buffer.readTo(destination)
        return ByteReadBuffer(destination)
    }

    override fun close() {
        socket.close()
    }
}
