package com.github.clasicrando.common.stream

import com.github.clasicrando.common.DefaultUniqueResourceId
import com.github.clasicrando.common.buffer.ByteReadBuffer
import com.github.clasicrando.common.buffer.ByteWriteBuffer
import io.ktor.network.sockets.InetSocketAddress
import io.ktor.network.sockets.toJavaAddress
import java.net.Socket
import kotlin.time.Duration

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
        val buffer = ByteReadBuffer(inputStream.readNBytes(4))
        return buffer.readInt()
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