package io.github.clasicrando.kdbc.core.stream

import io.github.clasicrando.kdbc.core.DefaultUniqueResourceId
import io.github.clasicrando.kdbc.core.buffer.ByteReadBuffer
import io.github.clasicrando.kdbc.core.buffer.ByteWriteBuffer
import io.ktor.network.sockets.InetSocketAddress
import io.ktor.network.sockets.toJavaAddress
import java.net.Socket
import kotlin.time.Duration

/** Java base [Socket] implementation of a [BlockingStream] */
class SocketBlockingStream(
    private val address: InetSocketAddress,
) : BlockingStream, DefaultUniqueResourceId() {
    private val socket = Socket()
    private val inputStream get() = socket.getInputStream()
    private val outputStream get() = socket.getOutputStream()

    override val isConnected: Boolean get() = socket.isConnected

    override fun connect(timeout: Duration) {
        socket.connect(address.toJavaAddress(), timeout.inWholeMilliseconds.toInt())
    }

    override fun writeBuffer(buffer: ByteWriteBuffer) {
        val bytes = buffer.copyToArray()
        outputStream.write(bytes)
    }

    override fun readByte(): Byte {
        check(isConnected) { "Cannot read from a stream that is not connected" }
        return socket.getInputStream().read().toByte()
    }

    override fun readInt(): Int {
        check(isConnected) { "Cannot read from a stream that is not connected" }
        val bytes = inputStream.readNBytes(4)
        return (
            (bytes[0].toInt() and 0xff shl 24)
            or (bytes[1].toInt() and 0xff shl 16)
            or (bytes[2].toInt() and 0xff shl 8)
            or (bytes[3].toInt() and 0xff))
    }

    override fun readBuffer(count: Int): ByteReadBuffer {
        check(isConnected) { "Cannot read from a stream that is not connected" }
        val result = ByteReadBuffer(inputStream.readNBytes(count))
        return result
    }

    override fun release() {
        socket.close()
    }
}