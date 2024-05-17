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
    private var inputStream: InputStream? = null
    private var outputStream: OutputStream? = null

    override val isConnected: Boolean get() = socket.isConnected

    override fun connect(timeout: Duration) {
        socket.connect(address.toJavaAddress(), timeout.inWholeMilliseconds.toInt())
        inputStream = socket.inputStream
        outputStream = socket.outputStream
    }

    override fun writeBuffer(buffer: ByteWriteBuffer) {
        checkNotNull(outputStream) { "Cannot write to a stream that is not connected" }
        val bytes = buffer.copyToArray()
        outputStream?.write(bytes)
    }

    override fun readByte(): Byte {
        checkNotNull(inputStream) { "Cannot read from a stream that is not connected" }
        return inputStream!!.read().toByte()
    }

    override fun readInt(): Int {
        checkNotNull(inputStream) { "Cannot read from a stream that is not connected" }
        val bytes = inputStream!!.readNBytes(4)
        return (
            (bytes[0].toInt() and 0xff shl 24)
            or (bytes[1].toInt() and 0xff shl 16)
            or (bytes[2].toInt() and 0xff shl 8)
            or (bytes[3].toInt() and 0xff))
    }

    override fun readBuffer(count: Int): ByteReadBuffer {
        checkNotNull(inputStream) { "Cannot read from a stream that is not connected" }
        val result = ByteReadBuffer(inputStream!!.readNBytes(count))
        return result
    }

    override fun release() {
        inputStream = null
        outputStream = null
        socket.close()
    }
}