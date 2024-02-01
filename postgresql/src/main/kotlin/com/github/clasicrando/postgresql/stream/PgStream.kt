package com.github.clasicrando.postgresql.stream

import com.github.clasicrando.common.SslMode
import com.github.clasicrando.common.waitOrError
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
import com.github.clasicrando.postgresql.row.PgRowFieldDescription
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
import kotlinx.atomicfu.AtomicBoolean
import kotlinx.atomicfu.atomic
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.Job
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.channels.SendChannel
import kotlinx.coroutines.isActive
import kotlinx.coroutines.job
import kotlinx.coroutines.launch
import kotlinx.coroutines.withTimeout
import kotlinx.uuid.UUID
import kotlinx.uuid.generateUUID
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.cancellation.CancellationException

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
    private val encoders: PgMessageEncoders = PgMessageEncoders(connectOptions.charset)
    /**
     * Container for server message decoders. Used to parse messages sent from the database server.
     */
    private val decoders: PgMessageDecoders = PgMessageDecoders(connectOptions.charset)

    /**
     * [Channel] used to pass [PgRowFieldDescription] data received from the server to query
     * executors.
     */
    private val rowDescriptionChannel = Channel<List<PgRowFieldDescription>>()
    /**
     * [Channel] used to pass [PgMessage.DataRow] messages received from the server to query
     * executors.
     */
    private val dataRowChannel = Channel<PgMessage.DataRow>(capacity = Channel.BUFFERED)
    /**
     * [Channel] used to pass [PgMessage.CommandComplete] messages received from the server to
     * query executors.
     */
    private val commandCompleteChannel = Channel<PgMessage.CommandComplete>()
    /**
     * [Channel] used to notify a request to close a prepared statement that the close was
     * completed
     */
    private val closeStatementChannel = Channel<Unit>()
    /** [Channel] used to notify a query request that the query is now complete */
    private val queryDoneChannel = Channel<Unit>()
    /** [Channel] used to notify a `COPY FROM` request that the server is now ready for data rows */
    private val copyInResponseChannel = Channel<Unit>()
    /** [Channel] used to notify a `COPY TO` request that the server will now send data rows */
    private val copyOutResponseChannel = Channel<Unit>()
    /**
     * [Channel] used to pass [PgMessage.CopyData] rows sent by the server to the `COPY TO`
     * executor.
     */
    private val copyDataChannel = Channel<PgMessage.CopyData>(capacity = Channel.BUFFERED)
    /** [Channel] used to notify a `COPY TO` request that the server is done sending data rows */
    private val copyDoneChannel = Channel<Unit>()
    /**
     * [Channel] used to pass [GeneralPostgresError] instances to command executors to notify them
     * of a [PgMessage.ErrorResponse] message from the server.
     */
    private val errorChannel = Channel<GeneralPostgresError>(capacity = Channel.BUFFERED)
    /** [Channel] used to store all server notifications that have not been processed */
    private val notificationsChannel = Channel<PgNotification>(capacity = Channel.BUFFERED)

    /**
     * [Channel] used to pass [PgRowFieldDescription] data received from the server to query
     * executors.
     */
    val rowDescription: ReceiveChannel<List<PgRowFieldDescription>> = rowDescriptionChannel
    /**
     * [Channel] used to pass [PgMessage.DataRow] messages received from the server to query
     * executors.
     */
    val dataRows: ReceiveChannel<PgMessage.DataRow> = dataRowChannel
    /**
     * [Channel] used to pass [PgMessage.CommandComplete] messages received from the server to
     * query executors.
     */
    val commandComplete: ReceiveChannel<PgMessage.CommandComplete> = commandCompleteChannel
    /**
     * [Channel] used to notify a request to close a prepared statement that the close was
     * completed
     */
    val closeStatement: ReceiveChannel<Unit> = closeStatementChannel
    /** [Channel] used to notify a query request that the query is now complete */
    val queryDone: ReceiveChannel<Unit> = queryDoneChannel
    /** [Channel] used to notify a `COPY FROM` request that the server is now ready for data rows */
    val copyInResponse: ReceiveChannel<Unit> = copyInResponseChannel
    /** [Channel] used to notify a `COPY TO` request that the server will now send data rows */
    val copyOutResponse : ReceiveChannel<Unit> = copyOutResponseChannel
    /**
     * [Channel] used to pass [PgMessage.CopyData] rows sent by the server to the `COPY TO`
     * executor.
     */
    val copyData: ReceiveChannel<PgMessage.CopyData> = copyDataChannel
    /** [Channel] used to notify a `COPY TO` request that the server is done sending data rows */
    val copyDone: ReceiveChannel<Unit> = copyDoneChannel
    /**
     * [Channel] used to pass [GeneralPostgresError] instances to command executors to notify them
     * of a [PgMessage.ErrorResponse] message from the server.
     */
    val errors: ReceiveChannel<GeneralPostgresError> = errorChannel
    /** [Channel] used to store all server notifications that have not been processed */
    val notifications: ReceiveChannel<PgNotification> = notificationsChannel

    /**
     * Thread safe flag indicating that the client requested a copy fail and the resulting server
     * error message should not throw an exception when received.
     */
    private val copyFailed: AtomicBoolean = atomic(false)
    /** Thread safe flag indicating that a user requested a prepared statement to be released */
    private val releasePreparedStatement: AtomicBoolean = atomic(false)

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

    private val messageReaderJob = launch(start = CoroutineStart.LAZY) {
        var cause: Throwable? = null
        try {
            while (isActive && this@PgStream.isConnected) {
                when (val message = receiveNextServerMessage()) {
                    is PgMessage.ErrorResponse -> onErrorMessage(message)
                    is PgMessage.NoticeResponse -> onNotice(message)
                    is PgMessage.NotificationResponse -> onNotification(message)
                    is PgMessage.ParameterStatus -> onParameterStatus(message)
                    is PgMessage.ReadyForQuery -> onReadyForQuery(message)
                    is PgMessage.BackendKeyData -> onBackendKeyData(message)
                    is PgMessage.DataRow -> onDataRow(message)
                    is PgMessage.RowDescription -> onRowDescription(message)
                    is PgMessage.CommandComplete -> onCommandComplete(message)
                    is PgMessage.CloseComplete -> onCloseComplete()
                    is PgMessage.CopyInResponse -> onCopyInResponse(message)
                    is PgMessage.CopyOutResponse -> onCopyOutResponse(message)
                    is PgMessage.CopyData -> onCopyData(message)
                    is PgMessage.CopyDone -> onCopyDone()
                    else -> {
                        log(Level.TRACE) {
                            this.message = "Received message: {message}"
                            payload = mapOf("message" to message)
                        }
                    }
                }
            }
        } catch(cancel: CancellationException) {
            cause = cancel
            throw cancel
        } catch (ex: Throwable) {
            cause = ex
            logger.atError {
                message = "Error within server message reader"
                this.cause = ex
            }
        } finally {
            closeChannels(cause)
            logger.atTrace {
                message = "Server message reader is closing"
            }
        }
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
     * Event handler method for [PgMessage.ErrorResponse] messages. Logs the error and sends the
     * error message wrapped in a [GeneralPostgresError] [Throwable] to the [errorChannel] unless
     * [copyFailed] flag is true. In that case, no error is sent and a [PgMessage.CommandComplete]
     * message is sent to [commandCompleteChannel] to notify the current `COPY FROM` operations
     * that that previous call failed.
     */
    private suspend fun onErrorMessage(message: PgMessage.ErrorResponse) {
        if (copyFailed.getAndSet(false)) {
            log(Level.WARN) {
                this.message = "CopyIn was failed by client -> {message}"
                payload = mapOf("message" to message)
            }
            val commandComplete = PgMessage.CommandComplete(0, "CopyIn failed by client")
            commandCompleteChannel.send(commandComplete)
            return
        }
        log(Level.ERROR) {
            this.message = "Error, message -> {errorResponse}"
            payload = mapOf("errorResponse" to message)
        }
        val throwable = GeneralPostgresError(message)
        errorChannel.send(throwable)
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

    /**
     * Event handler method for [PgMessage.ReadyForQuery] messages. Notifies the [queryDoneChannel]
     * that the current query operation is complete and the server is ready for more queries.
     */
    private suspend fun onReadyForQuery(message: PgMessage.ReadyForQuery) {
        log(Level.TRACE) {
            this.message = "Connection ready for query. Transaction Status: {status}"
            payload = mapOf("status" to message.transactionStatus)
        }
        queryDoneChannel.send(Unit)
    }

    /**
     * Event handler method for [PgMessage.RowDescription] messages. Sends the fields contains
     * within the [message] to the [rowDescriptionChannel].
     */
    private suspend fun onRowDescription(message: PgMessage.RowDescription) {
        log(Level.TRACE) {
            this.message = "Received row description -> {desc}"
            payload = mapOf("dec" to message)
        }
        rowDescriptionChannel.send(message.fields)
    }

    /**
     * Event handler method for [PgMessage.NoticeResponse] messages. Sends the [message] to the
     * [dataRowChannel].
     */
    private suspend fun onDataRow(message: PgMessage.DataRow) {
        log(Level.TRACE) {
            this.message = "Received row size = {size}"
            payload = mapOf("size" to message.values.size)
        }
        dataRowChannel.send(message)
    }

    /**
     * Event handler method for [PgMessage.CommandComplete] messages. Sends the [message] to the
     * [commandCompleteChannel].
     */
    private suspend fun onCommandComplete(message: PgMessage.CommandComplete) {
        log(Level.TRACE) {
            this.message = "Received command complete -> {message}"
            payload = mapOf("message" to message)
        }
        commandCompleteChannel.send(message)
    }

    /**
     * Event handler method for [PgMessage.NoticeResponse] messages. Checks to see if the
     * [releasePreparedStatement] is true. If yes, then [closeStatementChannel] is used to notify
     * the release requester of the completion. Otherwise, the message is ignored. In all cases,
     * the [releasePreparedStatement] is reverted to false.
     */
    private suspend fun onCloseComplete() {
        log(Level.TRACE) {
            this.message = "Received close complete"
        }
        if (releasePreparedStatement.getAndSet(false)) {
            closeStatementChannel.send(Unit)
        }
    }

    /**
     * Event handler method for [PgMessage.CopyInResponse] messages. Notifies the
     * [copyInResponseChannel] that the server is ready to proceed.
     */
    private suspend fun onCopyInResponse(message: PgMessage.CopyInResponse) {
        log(Level.TRACE) {
            this.message = "Received CopyInResponse -> {message}"
            payload = mapOf("message" to message)
        }
        copyInResponseChannel.send(Unit)
    }

    /**
     * Event handler method for [PgMessage.CopyInResponse] messages. Notifies the
     * [copyOutResponseChannel] that the server is ready to proceed.
     */
    private suspend fun onCopyOutResponse(message: PgMessage.CopyOutResponse) {
        log(Level.TRACE) {
            this.message = "Received CopyOutResponse -> {message}"
            payload = mapOf("message" to message)
        }
        copyOutResponseChannel.send(Unit)
    }

    /**
     * Event handler method for [PgMessage.CopyInResponse] messages. Sends the [message] to the
     * [copyDataChannel]
     */
    private suspend fun onCopyData(message: PgMessage.CopyData) {
        log(Level.TRACE) {
            this.message = "Received CopyData, size = {size}"
            payload = mapOf("size" to message.data.size)
        }
        copyDataChannel.send(message)
    }
    /**
     * Event handler method for [PgMessage.CopyInResponse] messages. Notifies the [copyDoneChannel]
     * that the server is done sending data.
     */
    private suspend fun onCopyDone() {
        log(Level.TRACE) {
            this.message = "Received CopyDone"
        }
        copyDoneChannel.send(Unit)
    }

    private val messageWriterChannel = Channel<PgWriteMessage>(capacity = Channel.BUFFERED)
    val messageWriter: SendChannel<PgWriteMessage> = messageWriterChannel

    init {
        launch {
            var cause: Throwable? = null
            try {
                for (message in messageWriterChannel) {
                    writeMessage(message)
                }
            } catch(cancel: CancellationException) {
                cause = cancel
                throw cancel
            } catch (ex: Throwable) {
                cause = ex
                logger.atError {
                    message = "Error within server message processor"
                    this.cause = ex
                }
                throw ex
            } finally {
                closeChannels(cause)
                logger.atTrace {
                    message = "Server Message Processor is closing"
                }
            }
        }
    }

    /** Write a single [message] to the [PgStream] using the appropriate derived encoder. */
    suspend inline fun writeToStream(message: PgMessage) {
        messageWriter.send(PgWriteMessage.Single(message))
    }

    /** Write multiple [messages] to the [PgStream] using the appropriate derived encoders. */
    suspend inline fun writeManyToStream(messages: Iterable<PgMessage>) {
        messageWriter.send(PgWriteMessage.Multiple(messages))
    }

    /** Write multiple [messages] to the [PgStream] using the appropriate derived encoders. */
    suspend inline fun writeManyToStream(vararg messages: PgMessage) {
        writeManyToStream(messages.asIterable())
    }

    /**
     * Close all channels held by this connection, supplying the [throwable] if it's the cause of
     * the closure.
     */
    private fun closeChannels(throwable: Throwable? = null) {
        rowDescriptionChannel.close(cause = throwable)
        dataRowChannel.close(cause = throwable)
        commandCompleteChannel.close(cause = throwable)
        queryDoneChannel.close(cause = throwable)
        closeStatementChannel.close(cause = throwable)
        copyOutResponseChannel.close(cause = throwable)
        copyInResponseChannel.close(cause = throwable)
        copyDataChannel.close(cause = throwable)
        copyDoneChannel.close(cause = throwable)
        errorChannel.close(cause = throwable)
        messageWriterChannel.close(throwable)
    }

    val isConnected: Boolean get() = connection.socket.isActive && !connection.socket.isClosed

    private suspend inline fun writeMessage(message: PgWriteMessage) =
        writeMessage { builder ->
            when (message) {
                is PgWriteMessage.Single -> {
                    val encoder = encoders.encoderFor(message.message)
                    encoder.encode(message.message, builder)
                }
                is PgWriteMessage.Multiple -> {
                    for (msg in message.messages) {
                        val encoder = encoders.encoderFor(msg)
                        encoder.encode(msg, builder)
                    }
                }
            }
        }

    private suspend inline fun writeMessage(block: (BytePacketBuilder) -> Unit) {
        sendChannel.writePacket(buildPacket(block))
        sendChannel.flush()
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
            onErrorMessage(message)
            return
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
            stream.messageReaderJob.start()
            waitOrError(stream.errors, stream.queryDone)
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