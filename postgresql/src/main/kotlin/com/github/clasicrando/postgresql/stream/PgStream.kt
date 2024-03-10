package com.github.clasicrando.postgresql.stream

import com.github.clasicrando.common.DefaultUniqueResourceId
import com.github.clasicrando.common.ExitOfProcessingLoop
import com.github.clasicrando.common.Loop
import com.github.clasicrando.common.buffer.ByteWriteBuffer
import com.github.clasicrando.common.message.SizedMessage
import com.github.clasicrando.common.resourceLogger
import com.github.clasicrando.common.stream.AsyncStream
import com.github.clasicrando.common.stream.Nio2AsyncStream
import com.github.clasicrando.common.stream.StreamConnectError
import com.github.clasicrando.common.stream.StreamReadError
import com.github.clasicrando.common.stream.StreamWriteError
import com.github.clasicrando.postgresql.GeneralPostgresError
import com.github.clasicrando.postgresql.authentication.Authentication
import com.github.clasicrando.postgresql.authentication.PgAuthenticationError
import com.github.clasicrando.postgresql.authentication.saslAuthFlow
import com.github.clasicrando.postgresql.authentication.simplePasswordAuthFlow
import com.github.clasicrando.postgresql.connection.PgConnectOptions
import com.github.clasicrando.postgresql.message.PgMessage
import com.github.clasicrando.postgresql.message.decoders.PgMessageDecoders
import com.github.clasicrando.postgresql.message.encoders.PgMessageEncoders
import com.github.clasicrando.postgresql.notification.PgNotification
import io.github.oshai.kotlinlogging.KLoggingEventBuilder
import io.github.oshai.kotlinlogging.KotlinLogging
import io.github.oshai.kotlinlogging.Level
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.isActive
import kotlinx.coroutines.job
import java.net.InetSocketAddress
import kotlin.coroutines.CoroutineContext

private val logger = KotlinLogging.logger {}
private const val RESOURCE_TYPE = "PgStream"

internal class PgStream(
    private val scope: CoroutineScope,
    private var connection: AsyncStream,
    internal val connectOptions: PgConnectOptions,
) : DefaultUniqueResourceId(), CoroutineScope, AutoCloseable {
    /** Data sent from the backend during connection initialization */
    private var backendKeyData: PgMessage.BackendKeyData? = null
    private val messageSendBuffer = ByteWriteBuffer(MESSAGE_BUFFER_SIZE)

    override val resourceType: String = RESOURCE_TYPE

    override val coroutineContext: CoroutineContext get() = Job(parent = scope.coroutineContext.job)

    /**
     * Container for server message encoders. Used to send messages before they are sent to the
     * server.
     */
    private val encoders: PgMessageEncoders = PgMessageEncoders()
    /**
     * Container for server message decoders. Used to parse messages sent from the database server.
     */
    private val decoders: PgMessageDecoders = PgMessageDecoders()
    /** [Channel] used to store all server notifications that have not been processed */
    private val notificationsChannel = Channel<PgNotification>(capacity = Channel.BUFFERED)
    /** [ReceiveChannel] used to store all server notifications that have not been processed */
    val notifications: ReceiveChannel<PgNotification> = notificationsChannel

    internal inline fun log(level: Level, crossinline block: KLoggingEventBuilder.() -> Unit) {
        logger.resourceLogger(this, level, block)
    }

    internal suspend fun receiveNextServerMessage(): PgMessage {
        val format = connection.readByte()
        val size = connection.readInt()
        val buffer = connection.readBuffer(size - 4)
        val rawMessage = RawMessage(format = format, size = size.toUInt(), contents = buffer)
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
                        Loop.Continue, Loop.Noop -> continue
                        Loop.Break -> break
                    }
                }
            }
        }
    }

    suspend inline fun <reified T : PgMessage> waitForOrError(): T {
        while (isActive && isConnected) {
            when (val message = receiveNextServerMessage()) {
                is PgMessage.NoticeResponse -> onNotice(message)
                is PgMessage.NotificationResponse -> onNotification(message)
                is PgMessage.ParameterStatus -> onParameterStatus(message)
                is PgMessage.BackendKeyData -> onBackendKeyData(message)
                is PgMessage.ErrorResponse -> throw GeneralPostgresError(message)
                is T -> return message
                else -> {
                    log(Level.TRACE) {
                        this.message = "Ignoring {message} since it's not an error or the desired type"
                        payload = mapOf("message" to message)
                    }
                }
            }
        }
        throw ExitOfProcessingLoop(T::class.qualifiedName!!)
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
        writeBuffer { buffer ->
            val encoder = encoders.encoderFor(message)
            encoder.encode(message, buffer)
        }
    }

    /** Write multiple [messages] to the [PgStream] using the appropriate derived encoders. */
    suspend inline fun writeManyToStream(
        crossinline messages: suspend SequenceScope<PgMessage>.() -> Unit,
    ) {
        writeBuffer { buffer ->
            for (message in sequence { messages() }) {
                val encoder = encoders.encoderFor(message)
                encoder.encode(message, buffer)
            }
        }
    }

    /** Write multiple [messages] to the [PgStream] using the appropriate derived encoders. */
    suspend inline fun writeManyToStream(vararg messages: PgMessage) {
        writeBuffer { buffer ->
            for (message in messages) {
                val encoder = encoders.encoderFor(message)
                encoder.encode(message, buffer)
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

    val isConnected: Boolean get() = connection.isConnected

    private suspend inline fun writeBuffer(crossinline block: (ByteWriteBuffer) -> Unit) {
        try {
            messageSendBuffer.release()
            block(messageSendBuffer)
            messageSendBuffer.writeToAsyncStream(connection)
        } finally {
            messageSendBuffer.release()
        }
    }

    suspend fun <M> writeManySized(flow: Flow<M>)
    where
        M : SizedMessage,
        M : PgMessage
    {
        try {
            messageSendBuffer.release()
            flow.collect {
                if (messageSendBuffer.remaining < it.size) {
                    messageSendBuffer.writeToAsyncStream(connection)
                    messageSendBuffer.release()
                }
                val encoder = encoders.encoderFor(it)
                encoder.encode(it, messageSendBuffer)
            }
            if (messageSendBuffer.position > 0) {
                messageSendBuffer.writeToAsyncStream(connection)
            }
        } finally {
            messageSendBuffer.release()
        }
    }

    override fun close() {
        messageSendBuffer.release()
        closeChannels()
        if (connection.isConnected) {
            connection.release()
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
        val message = receiveNextServerMessage()
        if (message is PgMessage.ErrorResponse) {
            throw GeneralPostgresError(message)
        }
        if (message !is PgMessage.Authentication) {
            throw PgAuthenticationError(
                "Server sent non-auth message that was not an error. Closing connection"
            )
        }
        when (val auth = message.authentication) {
            Authentication.Ok -> {
                log(Level.TRACE) {
                    this.message = "Successfully logged in to database"
                }
            }
            Authentication.CleartextPassword -> {
                this.simplePasswordAuthFlow(
                    connectOptions.username,
                    connectOptions.password ?: throw PgAuthenticationError("Missing Password"),
                )
            }
            is Authentication.Md5Password -> {
                this.simplePasswordAuthFlow(
                    connectOptions.username,
                    connectOptions.password ?: throw PgAuthenticationError("Missing Password"),
                    auth.salt,
                )
            }
            is Authentication.Sasl -> this.saslAuthFlow(auth)
            else -> error("Auth request type cannot be handled. $auth")
        }
    }

//    private suspend fun requestUpgrade(): Boolean {
//        writeBuffer { builder ->
//            SslMessageEncoder.encode(PgMessage.SslRequest, builder)
//        }
//        return when (val response = receiveChannel.readByte()) {
//            'S'.code.toByte() -> true
//            'N'.code.toByte() -> false
//            else -> {
//                val responseChar = response.toInt().toChar()
//                error("Invalid response byte after SSL request. Byte = '$responseChar'")
//            }
//        }
//    }

//    @Suppress("BlockingMethodInNonBlockingContext", "UNUSED")
//    private suspend fun upgradeIfNeeded(
//        selectorManager: SelectorManager,
//        connectOptions: PgConnectOptions,
//    ) {
//        when (connectOptions.sslMode) {
//            SslMode.Disable, SslMode.Allow -> return
//            SslMode.Prefer -> {
//                if (!requestUpgrade()) {
//                    logger.atWarn {
//                        message = TLS_REJECT_WARNING
//                    }
//                    return
//                }
//            }
//            SslMode.Require, SslMode.VerifyCa, SslMode.VerifyFull -> {
//                check(requestUpgrade()) {
//                    "TLS connection required by client but server does not accept TSL connection"
//                }
//            }
//        }
//        connection.socket.close()
//        val newConnection = createConnection(
//            selectorManager = selectorManager,
//            connectOptions = connectOptions,
//        )
//        connection = newConnection
//    }

    companion object {
        private const val MESSAGE_BUFFER_SIZE = 2048
        private const val TLS_REJECT_WARNING = "Preferred SSL mode was rejected by server. " +
                "Continuing with non TLS connection"

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
         *
         * @throws PgAuthenticationError if the authentication fails
         * @throws StreamConnectError if the underling [AsyncStream] fails to connect
         * @throws StreamReadError if any authentication message or the final ready for query
         * message fails
         * @throws StreamWriteError if the startup message or any authentication message written to
         * the stream fails
         * @throws ExitOfProcessingLoop if waiting for [PgMessage.ReadyForQuery] or error after
         * authentication exits the processing loop unexpectedly
         */
        internal suspend fun connect(
            scope: CoroutineScope,
            connectOptions: PgConnectOptions,
        ): PgStream {
            val address = InetSocketAddress(connectOptions.host, connectOptions.port.toInt())
            val socket = Nio2AsyncStream(address)
            socket.connect()
            val stream = PgStream(scope, socket, connectOptions)
            val startupMessage = PgMessage.StartupMessage(params = connectOptions.properties)
            stream.writeToStream(startupMessage)
            stream.handleAuthFlow()
            stream.waitForOrError<PgMessage.ReadyForQuery>()
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