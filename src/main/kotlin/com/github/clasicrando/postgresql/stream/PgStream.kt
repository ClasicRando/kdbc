package com.github.clasicrando.postgresql.stream

import com.github.clasicrando.common.SslMode
import com.github.clasicrando.postgresql.PgConnectOptions
import com.github.clasicrando.postgresql.message.PgMessage
import com.github.clasicrando.postgresql.message.encoders.SslMessageEncoder
import io.klogging.Klogging
import io.ktor.network.selector.SelectorManager
import io.ktor.network.sockets.Connection
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

class PgStream(private var connection: Connection) : Klogging, AutoCloseable {
    private val receiveChannel get() = connection.input
    private val sendChannel get() = connection.output

    val isActive: Boolean get() = connection.socket.isActive && !connection.socket.isClosed

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
        sendChannel.flush()
    }

    override fun close() {
        connection.socket.close()
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
                    logger.warn(TLS_REJECT_WARNING)
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
        connection = createConnection(
            coroutineContext = coroutineContext,
            connectOptions = connectOptions,
        )
    }

    companion object {
        private const val TLS_REJECT_WARNING = "Preferred SSL mode was rejected by server. " +
                "Continuing with non TLS connection"

        private suspend fun createConnection(
            coroutineContext: CoroutineContext,
            connectOptions: PgConnectOptions,
            tlsConfig: (TLSConfigBuilder.() -> Unit)? = null,
        ): Connection {
            val selectorManager = SelectorManager(coroutineContext)
            val builder = aSocket(selectorManager).tcp()
            val socket = withTimeout(connectOptions.connectionTimeout.toLong()) {
                builder.connect(connectOptions.host, connectOptions.port.toInt()) {
                    keepAlive = true
                }
            }

            tlsConfig?.let {
                return socket.tls(coroutineContext, block = it).connection()
            }
            return socket.connection()
        }

        suspend fun connect(coroutineScope: CoroutineScope, connectOptions: PgConnectOptions): PgStream {
            val socket = createConnection(
                coroutineContext = coroutineScope.coroutineContext,
                connectOptions = connectOptions,
            )
            return PgStream(socket)
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