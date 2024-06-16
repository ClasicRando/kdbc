package io.github.clasicrando.kdbc.core.stream

import io.github.clasicrando.kdbc.core.DefaultUniqueResourceId
import io.github.clasicrando.kdbc.core.buffer.ByteReadBuffer
import io.github.clasicrando.kdbc.core.buffer.ByteWriteBuffer
import io.ktor.network.sockets.InetSocketAddress
import io.ktor.network.sockets.toJavaAddress
import java.io.InputStream
import java.io.OutputStream
import java.net.Socket
import kotlin.time.Duration

/** Java base [Socket] implementation of a [BlockingStream] */
class SocketBlockingStream(
    private val address: InetSocketAddress,
) : BlockingStream, DefaultUniqueResourceId() {
    private val socket = Socket()
    private lateinit var inputStream: InputStream
    private lateinit var outputStream: OutputStream

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

    override fun readByte(): Byte {
        check(isConnected) { "Cannot read from a stream that is not connected" }
        return inputStream.read().toByte()
    }

    override fun readInt(): Int {
        check(isConnected) { "Cannot read from a stream that is not connected" }
        return (
            (inputStream.read() and 0xff shl 24)
            or (inputStream.read() and 0xff shl 16)
            or (inputStream.read() and 0xff shl 8)
            or (inputStream.read() and 0xff))
    }

    override fun readBuffer(count: Int): ByteReadBuffer {
        check(isConnected) { "Cannot read from a stream that is not connected" }
        val result = ByteReadBuffer(inputStream.readNBytes(count))
        return result
    }

    override fun release() {
        if (this::inputStream.isInitialized) {
            inputStream.close()
        }
        if (this::outputStream.isInitialized) {
            outputStream.close()
        }
        socket.close()
    }
}