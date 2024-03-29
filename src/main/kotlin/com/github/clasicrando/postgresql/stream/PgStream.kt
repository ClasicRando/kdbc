package com.github.clasicrando.postgresql.stream

import com.github.clasicrando.common.SslMode
import com.github.clasicrando.postgresql.PgConnectOptions
import com.github.clasicrando.postgresql.message.PgMessage
import com.github.clasicrando.postgresql.message.encoders.SslMessageEncoder
import io.github.oshai.kotlinlogging.KotlinLogging
import io.ktor.network.selector.SelectorManager
import io.ktor.network.sockets.Connection
import io.ktor.network.sockets.Socket
import io.ktor.network.sockets.aSocket
import io.ktor.network.sockets.connection
import io.ktor.network.sockets.isClosed
import io.ktor.network.tls.TLSConfigBuilder
import io.ktor.network.tls.tls
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.isActive
import kotlinx.coroutines.withTimeout
import java.nio.ByteBuffer
import kotlin.coroutines.CoroutineContext

private val logger = KotlinLogging.logger {}

class PgStream(
    private var connection: Connection,
    private var selectorManager: SelectorManager,
): AutoCloseable {
    private val receiveChannel get() = connection.input
    private val sendChannel get() = connection.output

    val isActive: Boolean get() = connection.socket.isActive && !connection.socket.isClosed

    suspend fun receiveMessage(): RawMessage {
        receiveChannel.awaitContent()
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
        sendChannel.flush()
    }

    override fun close() {
        if (connection.socket.isActive) {
            connection.socket.close()
        }
        if (selectorManager.isActive) {
            selectorManager.close()
        }
    }

    private suspend fun requestUpgrade(): Boolean {
        writeMessage {
            SslMessageEncoder.encode(PgMessage.SslRequest, it)
        }
        return when (val response = receiveChannel.readByte()) {
            'S'.code.toByte() -> true
            'N'.code.toByte() -> false
            else -> {
                val responseChar = response.toInt().toChar()
                error("Invalid response byte after SSL request. Byte = '$responseChar'")
            }
        }
    }

    @Suppress("BlockingMethodInNonBlockingContext", "UNUSED")
    private suspend fun upgradeIfNeeded(
        coroutineContext: CoroutineContext,
        connectOptions: PgConnectOptions,
    ) {
        when (connectOptions.sslMode) {
            SslMode.Disable, SslMode.Allow -> return
            SslMode.Prefer -> {
                if (!requestUpgrade()) {
                    logger.atWarn {
                        message = TLS_REJECT_WARNING
                    }
                    return
                }
            }
            SslMode.Require, SslMode.VerifyCa, SslMode.VerifyFull -> {
                check(requestUpgrade()) {
                    "TLS connection required by client but server does not accept TSL connection"
                }
            }
        }
        connection.socket.close()
        val newConnection = createConnection(
            coroutineContext = coroutineContext,
            connectOptions = connectOptions,
        )
        selectorManager = newConnection.first
        connection = newConnection.second
    }

    companion object {
        private const val TLS_REJECT_WARNING = "Preferred SSL mode was rejected by server. " +
                "Continuing with non TLS connection"

        @Suppress("BlockingMethodInNonBlockingContext")
        private suspend fun createConnection(
            coroutineContext: CoroutineContext,
            connectOptions: PgConnectOptions,
            tlsConfig: (TLSConfigBuilder.() -> Unit)? = null,
        ): Pair<SelectorManager, Connection> {
            var selectorManager: SelectorManager? = null
            var socket: Socket? = null
            try {
                selectorManager = SelectorManager(coroutineContext)
                socket = withTimeout(connectOptions.connectionTimeout.toLong()) {
                    aSocket(selectorManager)
                        .tcp()
                        .connect(connectOptions.host, connectOptions.port.toInt()) {
                            keepAlive = true
                        }
                }

                tlsConfig?.let {
                    return selectorManager to socket.tls(coroutineContext, block = it).connection()
                }
                return selectorManager to socket.connection()
            } catch (ex: Throwable) {
                if (socket?.isActive == true) {
                    socket.close()
                }
                if (selectorManager?.isActive == true) {
                    selectorManager.close()
                }
                throw ex
            }
        }

        suspend fun connect(coroutineScope: CoroutineScope, connectOptions: PgConnectOptions): PgStream {
            val (selectorManager, socket) = createConnection(
                coroutineContext = coroutineScope.coroutineContext,
                connectOptions = connectOptions,
            )
            return PgStream(socket, selectorManager)
            /***
             * TODO
             * Need to add ability to upgrade to SSL connection, currently do not understand how
             * that is done with specified cert details with ktor-io
             */
//            stream.upgradeIfNeeded(
//                coroutineContext = coroutineScope.coroutineContext,
//                connectOptions = connectOptions,
//            )
//            return stream
        }
    }
}