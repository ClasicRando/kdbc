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
import io.ktor.utils.io.InternalAPI
import io.ktor.utils.io.readByte
import io.ktor.utils.io.readFully
import io.ktor.utils.io.readInt
import io.ktor.utils.io.writeFully
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.job
import kotlinx.coroutines.withTimeout
import kotlinx.io.Sink
import kotlin.coroutines.CoroutineContext
import kotlin.time.Duration

private val logger = KotlinLogging.logger {}

public class KtorStream(
    private val address: SocketAddress,
    private val selectorManager: SelectorManager,
    private val socketTimeout: Duration,
) : DefaultUniqueResourceId(), Stream {
    private lateinit var connection: Connection
    private lateinit var socket: Socket
    @PublishedApi internal lateinit var writeChannel: ByteWriteChannel
    private lateinit var readChannel: ByteReadChannel

    override val isConnected: Boolean
        get() = this::connection.isInitialized && !socket.isClosed

    override var coroutineContext: CoroutineContext = SupervisorJob()
        private set

    override val resourceType: String = "KtorStream"

    override suspend fun connect(timeout: Duration) {
        require(timeout.isPositive()) { "Timeout must be positive" }
        try {
            connection =
                withTimeout(timeout) {
                    aSocket(selectorManager)
                        .tcp()
                        .connect(address) {
                            this.socketTimeout = this@KtorStream.socketTimeout.inWholeMilliseconds
                        }
                        .connection()
                }
            socket = connection.socket
            writeChannel = connection.output
            readChannel = connection.input
            coroutineContext =
                socket.coroutineContext + SupervisorJob(parent = socket.coroutineContext.job)
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
                connection.tls(coroutineContext = selectorManager.coroutineContext).connection()
            }
        socket = connection.socket
        writeChannel = connection.output
        readChannel = connection.input
    }

    private suspend inline fun useWriteChannelFlushing(
        crossinline block: suspend (ByteWriteChannel) -> Unit
    ) {
        check(isConnected) { "Cannot write to a stream that is not connected" }
        var error: Exception? = null
        try {
            block(writeChannel)
        } catch (ex: Exception) {
            error = ex
        } finally {
            if (error == null) {
                try {
                    writeChannel.flush()
                } catch (ex: Exception) {
                    error = ex
                }
            }
        }

        if (error != null) {
            throw StreamWriteError(error)
        }
    }

    @OptIn(InternalAPI::class)
    override suspend fun writeTo(block: suspend (Sink) -> Unit) {
        useWriteChannelFlushing { block(it.writeBuffer) }
    }

    override suspend fun write(buffer: ByteWriteBuffer) {
        useWriteChannelFlushing {
            try {
                it.writeFully(value = buffer.innerBuffer, startIndex = 0, endIndex = buffer.offset)
            } finally {
                buffer.reset()
            }
        }
    }

    override suspend fun readByte(): Byte {
        check(isConnected) { "Cannot read from a stream that is not connected" }
        return try {
            readChannel.readByte()
        } catch (ex: Exception) {
            throw StreamReadError(ex)
        }
    }

    override suspend fun readInt(): Int {
        check(isConnected) { "Cannot read from a stream that is not connected" }
        return try {
            readChannel.readInt()
        } catch (ex: Exception) {
            throw StreamReadError(ex)
        }
    }

    override suspend fun readBuffer(count: Int): ByteReadBuffer {
        check(isConnected) { "Cannot read from a stream that is not connected" }
        val destination = ByteArray(count)
        try {
            readChannel.readFully(destination)
        } catch (ex: Exception) {
            throw StreamReadError(ex)
        }
        return ByteReadBuffer(destination)
    }

    override suspend fun readIntoBuffer(buffer: ByteWriteBuffer, count: Int) {
        check(isConnected) { "Cannot read from a stream that is not connected" }
        try {
            readChannel.readFully(
                out = buffer.innerBuffer,
                start = buffer.offset,
                end = buffer.offset + count,
            )
        } catch (ex: Exception) {
            throw StreamReadError(ex)
        }
        buffer.offset += count
    }

    override fun close() {
        socket.close()
    }
}
