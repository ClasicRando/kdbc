package io.github.clasicrando.kdbc.core.stream

import io.github.clasicrando.kdbc.core.DefaultUniqueResourceId
import io.github.clasicrando.kdbc.core.buffer.ByteReadBuffer
import io.github.clasicrando.kdbc.core.buffer.ByteWriteBuffer
import io.github.clasicrando.kdbc.core.config.Kdbc
import io.github.clasicrando.kdbc.core.logWithResource
import io.github.oshai.kotlinlogging.KotlinLogging
import io.ktor.network.sockets.InetSocketAddress
import io.ktor.network.sockets.toJavaAddress
import kotlinx.coroutines.TimeoutCancellationException
import kotlinx.io.Buffer
import kotlinx.io.readTo
import java.io.InputStream
import java.io.OutputStream
import java.net.Socket
import kotlin.time.Duration

private val logger = KotlinLogging.logger {}

/** Java base [Socket] implementation of a [BlockingStream] */
class SocketBlockingStream(
    private val address: InetSocketAddress,
) : BlockingStream, DefaultUniqueResourceId() {
    private val socket = Socket()
    private lateinit var inputStream: InputStream
    private lateinit var outputStream: OutputStream
    private val buffer = Buffer()
    private val tempBuffer = ByteArray(DEFAULT_BUFFER_SIZE)

    override val isConnected: Boolean get() = socket.isConnected

    override fun connect(timeout: Duration) {
        socket.connect(address.toJavaAddress(), timeout.inWholeMilliseconds.toInt())
        inputStream = socket.inputStream
        outputStream = socket.outputStream
    }

    override fun writeBuffer(buffer: ByteWriteBuffer) {
        check(isConnected) { "Cannot read from a stream that is not connected" }
        val bytes = buffer.copyToArray()
        outputStream.write(bytes)
    }

    private fun readIntoBuffer(required: Long) {
        var bytesRequired = required
        while (true) {
            val bytesRead = try {
                inputStream.read(tempBuffer)
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
                message = "Received {count} bytes from {address}"
                payload = mapOf("count" to bytesRead, "address" to address)
            }

            buffer.write(tempBuffer, 0, bytesRead)
            bytesRequired -= bytesRead
            if (bytesRequired <= 0) {
                return
            }
        }
    }

    override fun readByte(): Byte {
        check(isConnected) { "Cannot read from a stream that is not connected" }
        if (buffer.size < 1L) {
            readIntoBuffer(1)
        }
        return buffer.readByte()
    }

    override fun readInt(): Int {
        check(isConnected) { "Cannot read from a stream that is not connected" }
        if (buffer.size < 4) {
            readIntoBuffer(4 - buffer.size)
        }
        return buffer.readInt()
    }

    override fun readBuffer(count: Int): ByteReadBuffer {
        check(isConnected) { "Cannot read from a stream that is not connected" }
        val destination = ByteArray(count)

        if (buffer.size < count) {
            readIntoBuffer(count - buffer.size)
        }
        buffer.readTo(destination)
        return ByteReadBuffer(destination)
    }

    override fun close() {
        if (this::inputStream.isInitialized) {
            inputStream.close()
        }
        if (this::outputStream.isInitialized) {
            outputStream.close()
        }
        socket.close()
    }
}