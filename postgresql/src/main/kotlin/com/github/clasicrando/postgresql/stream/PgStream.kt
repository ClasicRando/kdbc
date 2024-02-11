package com.github.clasicrando.postgresql.stream

import com.github.clasicrando.common.Loop
import com.github.clasicrando.common.SslMode
import com.github.clasicrando.postgresql.GeneralPostgresError
import com.github.clasicrando.postgresql.authentication.Authentication
import com.github.clasicrando.postgresql.authentication.saslAuthFlow
import com.github.clasicrando.postgresql.authentication.simplePasswordAuthFlow
import com.github.clasicrando.postgresql.connection.PgConnectOptions
import com.github.clasicrando.postgresql.message.PgMessage
import com.github.clasicrando.postgresql.message.decoders.PgMessageDecoders
import com.github.clasicrando.postgresql.message.encoders.PgMessageEncoders
import com.github.clasicrando.postgresql.message.encoders.SslMessageEncoder
import com.github.clasicrando.postgresql.notification.PgNotification
import io.github.oshai.kotlinlogging.KLoggingEventBuilder
import io.github.oshai.kotlinlogging.KotlinLogging
import io.github.oshai.kotlinlogging.Level
import io.ktor.network.selector.SelectorManager
import io.ktor.network.sockets.Connection
import io.ktor.network.sockets.Socket
import io.ktor.network.sockets.aSocket
import io.ktor.network.sockets.connection
import io.ktor.network.sockets.isClosed
import io.ktor.network.tls.TLSConfigBuilder
import io.ktor.network.tls.tls
import io.ktor.utils.io.core.BytePacketBuilder
import io.ktor.utils.io.core.buildPacket
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.isActive
import kotlinx.coroutines.job
import kotlinx.coroutines.withTimeout
import kotlinx.uuid.UUID
import kotlinx.uuid.generateUUID
import kotlin.coroutines.CoroutineContext

private val logger = KotlinLogging.logger {}

internal class PgStream(
    private val scope: CoroutineScope,
    private var connection: Connection,
    internal val connectOptions: PgConnectOptions,
): CoroutineScope, AutoCloseable {
    private val receiveChannel get() = connection.input
    private val sendChannel get() = connection.output
    private val pgStreamId: UUID = UUID.generateUUID()
    /** Data sent from the backend during connection initialization */
    private var backendKeyData: PgMessage.BackendKeyData? = null

    override val coroutineContext: CoroutineContext get() = Job(parent = scope.coroutineContext.job)

    /**
     * Container for server message encoders. Used to send messages before they are sent to the
     * server.
     */
    private val encoders: PgMessageEncoders = PgMessageEncoders(Charsets.UTF_8)
    /**
     * Container for server message decoders. Used to parse messages sent from the database server.
     */
    private val decoders: PgMessageDecoders = PgMessageDecoders(Charsets.UTF_8)
    /** [Channel] used to store all server notifications that have not been processed */
    private val notificationsChannel = Channel<PgNotification>(capacity = Channel.BUFFERED)
    /** [ReceiveChannel] used to store all server notifications that have not been processed */
    val notifications: ReceiveChannel<PgNotification> = notificationsChannel

    internal inline fun log(level: Level, crossinline block: KLoggingEventBuilder.() -> Unit) {
        val connectionId = "connectionId" to pgStreamId.toString()
        logger.at(level) {
            block()
            payload = payload?.plus(connectionId) ?: mapOf(connectionId)
        }
    }

    internal suspend fun receiveNextServerMessage(): PgMessage {
        val format = receiveChannel.readByte()
        val size = receiveChannel.readInt()
        val packet = receiveChannel.readPacket(size - 4)
        val rawMessage = RawMessage(
            format = format,
            size = size.toUInt(),
            contents = packet,
        )
        return decoders.decode(rawMessage)
    }

    suspend inline fun processMessageLoop(process: (PgMessage) -> Loop) {
        while (isActive && isConnected) {
            when (val message = receiveNextServerMessage()) {
                is PgMessage.NoticeResponse -> onNotice(message)
                is PgMessage.NotificationResponse -> onNotification(message)
                is PgMessage.ParameterStatus -> onParameterStatus(message)
                is PgMessage.BackendKeyData -> onBackendKeyData(message)
                else -> {
                    when (process(message)) {
                        Loop.Continue -> continue
                        Loop.Break -> break
                    }
                }
            }
        }
    }

    suspend inline fun <reified T : PgMessage> waitForOrError(): Result<T> {
        while (isActive && isConnected) {
            when (val message = receiveNextServerMessage()) {
                is PgMessage.NoticeResponse -> onNotice(message)
                is PgMessage.NotificationResponse -> onNotification(message)
                is PgMessage.ParameterStatus -> onParameterStatus(message)
                is PgMessage.BackendKeyData -> onBackendKeyData(message)
                is PgMessage.ErrorResponse -> throw GeneralPostgresError(message)
                is T -> return Result.success(message)
                else -> {
                    log(Level.TRACE) {
                        this.message = "Ignoring {message} since it's not an error or the desired type"
                        payload = mapOf("message" to message)
                    }
                }
            }
        }
        val error = IllegalStateException("Unexpected exit waiting for PgMessage.RowDescription")
        return Result.failure(error)
    }

    /**
     * Event handler method for [PgMessage.NoticeResponse] messages. Only logs the message details
     */
    private fun onNotice(message: PgMessage.NoticeResponse) {
        log(Level.TRACE) {
            this.message = "Notice, message -> {noticeResponse}"
            payload = mapOf("noticeResponse" to message)
        }
    }

    /**
     * Event handler method for [PgMessage.NotificationResponse] messages. Creates a new
     * [PgNotification] instance and passes that to [notificationsChannel]
     */
    private suspend fun onNotification(message: PgMessage.NotificationResponse) {
        val notification = PgNotification(message.channelName, message.payload)
        log(Level.TRACE) {
            this.message = "Notification, message -> {notification}"
            payload = mapOf("notification" to message)
        }
        notificationsChannel.send(notification)
    }

    /**
     * Event handler method for [PgMessage.BackendKeyData] messages. Puts the [message] contents
     * into [backendKeyData].
     */
    private fun onBackendKeyData(message: PgMessage.BackendKeyData) {
        log(Level.TRACE) {
            this.message = "Got backend key data. Process ID: {processId}, Secret Key: ****"
            payload = mapOf("payloadId" to message.processId)
        }
        backendKeyData = message
    }

    /**
     * Event handler method for [PgMessage.ParameterStatus] messages. Only logs the message details
     */
    private fun onParameterStatus(message: PgMessage.ParameterStatus) {
        log(Level.TRACE) {
            this.message = "Parameter Status, {status}"
            payload = mapOf("status" to message)
        }
    }

    /** Write a single [message] to the [PgStream] using the appropriate derived encoder. */
    suspend inline fun writeToStream(message: PgMessage) {
        writeMessage { builder ->
            val encoder = encoders.encoderFor(message)
            encoder.encode(message, builder)
        }
    }

    /** Write multiple [messages] to the [PgStream] using the appropriate derived encoders. */
    suspend inline fun writeManyToStream(
        crossinline messages: suspend SequenceScope<PgMessage>.() -> Unit,
    ) {
        writeMessage { builder ->
            for (message in sequence { messages() }) {
                val encoder = encoders.encoderFor(message)
                encoder.encode(message, builder)
            }
        }
    }

    /** Write multiple [messages] to the [PgStream] using the appropriate derived encoders. */
    suspend inline fun writeManyToStream(vararg messages: PgMessage) {
        writeMessage { builder ->
            for (message in messages) {
                val encoder = encoders.encoderFor(message)
                encoder.encode(message, builder)
            }
        }
    }

    /**
     * Close all channels held by this connection, supplying the [throwable] if it's the cause of
     * the closure.
     */
    private fun closeChannels(throwable: Throwable? = null) {
        notificationsChannel.close(throwable)
    }

    val isConnected: Boolean get() = connection.socket.isActive && !connection.socket.isClosed

    private suspend inline fun writeMessage(block: (BytePacketBuilder) -> Unit) {
        val packet = buildPacket(block)
        sendChannel.writePacket(packet)
        sendChannel.flush()
        packet.release()
    }

    override fun close() {
        closeChannels()
        if (connection.socket.isActive) {
            connection.socket.close()
        }
    }

    /**
     * Handle the incoming authentication request with the proper flow of messages. Currently, only
     * clear text password, md5 password and SASL flows are implemented so if the server requests
     * and different authentication flow then an exception will be thrown. This will also throw
     * an exception if the first received message is not a [PgMessage.Authentication] message.
     * In the event the first message is a [PgMessage.ErrorResponse] the [onErrorMessage] handler
     * is invoked which signals the [PgStream] initialization method that the connection should
     * be disposed.
     */
    private suspend fun handleAuthFlow() {
        val isAuthenticated: Boolean
        val message = receiveNextServerMessage()
        if (message is PgMessage.ErrorResponse) {
            throw GeneralPostgresError(message)
        }
        if (message !is PgMessage.Authentication) {
            error("Server sent non-auth message that was not an error. Closing connection")
        }
        when (val auth = message.authentication) {
            Authentication.Ok -> {
                log(Level.TRACE) {
                    this.message = "Successfully logged in to database"
                }
                isAuthenticated = true
            }
            Authentication.CleartextPassword -> {
                isAuthenticated = this.simplePasswordAuthFlow(
                    connectOptions.username,
                    connectOptions.password ?: error("Password must be provided"),
                )
            }
            is Authentication.Md5Password -> {
                isAuthenticated = this.simplePasswordAuthFlow(
                    connectOptions.username,
                    connectOptions.password ?: error("Password must be provided"),
                    auth.salt,
                )
            }
            is Authentication.Sasl -> {
                isAuthenticated = this.saslAuthFlow(auth)
            }
            else -> error("Auth request type cannot be handled. $auth")
        }
        if (!isAuthenticated) {
            error("Not authenticated")
        }
    }

    private suspend fun requestUpgrade(): Boolean {
        writeMessage { builder ->
            SslMessageEncoder.encode(PgMessage.SslRequest, builder)
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
        selectorManager: SelectorManager,
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
            selectorManager = selectorManager,
            connectOptions = connectOptions,
        )
        connection = newConnection
    }

    companion object {
        private const val TLS_REJECT_WARNING = "Preferred SSL mode was rejected by server. " +
                "Continuing with non TLS connection"

        @Suppress("BlockingMethodInNonBlockingContext")
        private suspend fun createConnection(
            selectorManager: SelectorManager,
            connectOptions: PgConnectOptions,
            tlsConfig: (TLSConfigBuilder.() -> Unit)? = null,
        ): Connection {
            var socket: Socket? = null
            try {
                socket = withTimeout(connectOptions.connectionTimeout.toLong()) {
                    aSocket(selectorManager)
                        .tcp()
                        .connect(connectOptions.host, connectOptions.port.toInt()) {
                            keepAlive = true
                        }
                }

                tlsConfig?.let {
                    return socket.tls(selectorManager.coroutineContext, block = it).connection()
                }
                return socket.connection()
            } catch (ex: Throwable) {
                if (socket?.isActive == true) {
                    socket.close()
                }
                if (selectorManager.isActive) {
                    selectorManager.close()
                }
                throw ex
            }
        }

        /**
         * Create a new TCP connection with the Postgresql database targeted by the
         * [connectOptions] provided. This creates the new [PgStream] instance which in turn starts
         * the background message writer job and lazily starts the [messageReaderJob].
         *
         * To initiate the Postgresql connection, a [PgMessage.StartupMessage] is sent and the
         * response is handled using [handleAuthFlow]. After the connection has been authenticated
         * The method waits for a [PgMessage.ReadyForQuery] server response. In the case that the
         * server rejects the connection (by sending a [PgMessage.ErrorResponse]) the new
         * [PgStream] object is closed and an exception is thrown.
         */
        internal suspend fun connect(
            selectorManager: SelectorManager,
            connectOptions: PgConnectOptions,
        ): PgStream {
            val socket = createConnection(
                selectorManager = selectorManager,
                connectOptions = connectOptions,
            )
            val stream = PgStream(selectorManager, socket, connectOptions)
            val startupMessage = PgMessage.StartupMessage(params = connectOptions.properties)
            stream.writeToStream(startupMessage)
            stream.handleAuthFlow()
            stream.waitForOrError<PgMessage.ReadyForQuery>()
                .getOrThrow()
            return stream
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