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
import io.ktor.utils.io.readByte
import io.ktor.utils.io.readFully
import io.ktor.utils.io.readInt
import io.ktor.utils.io.writePacket
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.job
import kotlinx.coroutines.withTimeout
import kotlinx.io.Buffer
import kotlin.coroutines.CoroutineContext
import kotlin.time.Duration

private val logger = KotlinLogging.logger {}

public class KtorStream(
    private val address: SocketAddress,
    private val selectorManager: SelectorManager,
) : DefaultUniqueResourceId(), Stream {
    private lateinit var connection: Connection
    private lateinit var socket: Socket
    private lateinit var writeChannel: ByteWriteChannel
    private lateinit var readChannel: ByteReadChannel

    override val isConnected: Boolean
        get() = this::connection.isInitialized && !socket.isClosed

    override var coroutineContext: CoroutineContext = SupervisorJob()
        private set

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

    override suspend fun writeBuffer(buffer: Buffer) {
        check(isConnected) { "Cannot write to a stream that is not connected" }
        try {
            writeChannel.writePacket(buffer)
        } finally {
            writeChannel.flush()
        }
    }

    override suspend fun readByte(): Byte {
        check(isConnected) { "Cannot read from a stream that is not connected" }
        return readChannel.readByte()
    }

    override suspend fun readInt(): Int {
        check(isConnected) { "Cannot read from a stream that is not connected" }
        return readChannel.readInt()
    }

    override suspend fun readBuffer(count: Int): ByteReadBuffer {
        check(isConnected) { "Cannot read from a stream that is not connected" }
        val destination = ByteArray(count)
        readChannel.readFully(destination)
        return ByteReadBuffer(destination)
    }

    override fun close() {
        socket.close()
    }
}
