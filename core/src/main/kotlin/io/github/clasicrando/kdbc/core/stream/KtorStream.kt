package io.github.clasicrando.kdbc.core.stream

import io.github.clasicrando.kdbc.core.DefaultUniqueResourceId
import io.github.clasicrando.kdbc.core.buffer.ByteReadBuffer
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
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.job
import kotlinx.coroutines.withTimeout
import kotlinx.io.Sink
import kotlin.coroutines.CoroutineContext
import kotlin.time.Duration

private const val RESOURCE_TYPE = "KtorStream"

private val logger = KotlinLogging.logger {}

public class KtorStream(
    private val address: SocketAddress,
    private val selectorManager: SelectorManager,
    private val socketTimeout: Duration,
) : DefaultUniqueResourceId(), Stream {
    private lateinit var connection: Connection
    private lateinit var socket: Socket
    private lateinit var writeChannel: ByteWriteChannel
    private lateinit var readChannel: ByteReadChannel

    override val resourceType: String = RESOURCE_TYPE

    override val isConnected: Boolean
        get() = this::connection.isInitialized && !socket.isClosed

    override var coroutineContext: CoroutineContext = SupervisorJob()
        private set

    override suspend fun connect(timeout: Duration) {
        require(timeout.isPositive()) { "Timeout must be positive" }
        try {
            connection =
                withTimeout(timeout) {
                    aSocket(selectorManager).tcp().connect(address) {
                        this.socketTimeout = this@KtorStream.socketTimeout.inWholeMilliseconds
                    }.connection()
                }
            socket = connection.socket
            writeChannel = connection.output
            readChannel = connection.input
            coroutineContext =
                socket.coroutineContext + SupervisorJob(parent = socket.coroutineContext.job)
        } catch (ex: Exception) {
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

    @OptIn(InternalAPI::class)
    override suspend fun writeTo(block: suspend (Sink) -> Unit) {
        check(isConnected) { "Cannot write to a stream that is not connected" }
        var error: Exception? = null
        try {
            block(writeChannel.writeBuffer)
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

    override fun close() {
        socket.close()
    }
}
