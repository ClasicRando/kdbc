package com.github.clasicrando.postgresql.stream

import com.github.clasicrando.postgresql.PgConnectOptions
import io.klogging.Klogging
import io.ktor.network.selector.SelectorManager
import io.ktor.network.sockets.Socket
import io.ktor.network.sockets.aSocket
import io.ktor.network.sockets.isClosed
import io.ktor.network.sockets.openReadChannel
import io.ktor.network.sockets.openWriteChannel
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.isActive
import kotlinx.coroutines.withTimeout
import java.nio.ByteBuffer

class PgStream(private val socket: Socket) : Klogging, AutoCloseable {
    private val receiveChannel = socket.openReadChannel()
    private val sendChannel = socket.openWriteChannel(autoFlush = true)

    val isActive: Boolean get() = socket.isActive && !socket.isClosed

    suspend fun receiveMessage(): RawMessage {
        val format = receiveChannel.readByte()
        val size = receiveChannel.readInt()
        val packet = receiveChannel.readPacket(size - 4)
        return RawMessage(
            format = format,
            size = size.toUInt(),
            contents = packet,
        )
    }

    suspend fun writeMessage(block: (ByteBuffer) -> Unit) {
        sendChannel.write(block = block)
    }

    override fun close() {
        socket.close()
    }

    companion object {
        suspend fun connect(coroutineScope: CoroutineScope, connectOptions: PgConnectOptions): PgStream {
            val selectorManager = SelectorManager(coroutineScope.coroutineContext)
            val builder = aSocket(selectorManager).tcp()
            val socket = withTimeout(connectOptions.connectionTimeout.toLong()) {
                builder.connect(connectOptions.host, connectOptions.port.toInt()) {
                    keepAlive = true
                }
            }
            return PgStream(socket)
        }
    }
}