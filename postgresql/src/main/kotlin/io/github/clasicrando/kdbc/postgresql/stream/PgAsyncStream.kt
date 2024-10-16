package io.github.clasicrando.kdbc.postgresql.stream

import io.github.clasicrando.kdbc.core.DefaultUniqueResourceId
import io.github.clasicrando.kdbc.core.ExitOfProcessingLoop
import io.github.clasicrando.kdbc.core.Loop
import io.github.clasicrando.kdbc.core.SslMode
import io.github.clasicrando.kdbc.core.buffer.ByteArrayWriteBuffer
import io.github.clasicrando.kdbc.core.buffer.ByteWriteBuffer
import io.github.clasicrando.kdbc.core.config.Kdbc
import io.github.clasicrando.kdbc.core.exceptions.KdbcException
import io.github.clasicrando.kdbc.core.logWithResource
import io.github.clasicrando.kdbc.core.message.SizedMessage
import io.github.clasicrando.kdbc.core.stream.AsyncStream
import io.github.clasicrando.kdbc.core.stream.StreamConnectError
import io.github.clasicrando.kdbc.core.stream.StreamReadError
import io.github.clasicrando.kdbc.core.stream.StreamWriteError
import io.github.clasicrando.kdbc.postgresql.GeneralPostgresError
import io.github.clasicrando.kdbc.postgresql.authentication.Authentication
import io.github.clasicrando.kdbc.postgresql.authentication.PgAuthenticationError
import io.github.clasicrando.kdbc.postgresql.authentication.saslAuthFlow
import io.github.clasicrando.kdbc.postgresql.authentication.simplePasswordAuthFlow
import io.github.clasicrando.kdbc.postgresql.connection.PgAsyncConnection
import io.github.clasicrando.kdbc.postgresql.connection.PgConnectOptions
import io.github.clasicrando.kdbc.postgresql.message.PgMessage
import io.github.clasicrando.kdbc.postgresql.message.decoders.PgMessageDecoders
import io.github.clasicrando.kdbc.postgresql.message.encoders.PgMessageEncoders
import io.github.clasicrando.kdbc.postgresql.notification.PgNotification
import io.github.oshai.kotlinlogging.KLoggingEventBuilder
import io.github.oshai.kotlinlogging.KotlinLogging
import io.github.oshai.kotlinlogging.Level
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.isActive
import kotlinx.coroutines.job
import kotlin.coroutines.CoroutineContext

private val logger = KotlinLogging.logger {}
private const val RESOURCE_TYPE = "PgStream"

/**
 * [AsyncStream] wrapper class for facilitating postgresql specific message protocol behaviour.
 * A [PgAsyncConnection] will own a [PgAsyncStream] and utilize it's public methods to
 * process incoming server messages.
 */
internal class PgAsyncStream(
    scope: CoroutineScope,
    private val asyncStream: AsyncStream,
    internal val connectOptions: PgConnectOptions,
) : DefaultUniqueResourceId(), CoroutineScope, AutoCloseable {
    /** Data sent from the backend during connection initialization */
    private var backendKeyData: PgMessage.BackendKeyData? = null
    /** Reusable buffer for writing messages to the database server */
    private val messageSendBuffer: ByteWriteBuffer = ByteArrayWriteBuffer(SEND_BUFFER_SIZE)

    override val resourceType: String = RESOURCE_TYPE

    override val coroutineContext: CoroutineContext = Job(parent = scope.coroutineContext.job)

    /** [Channel] used to store all server notifications that have not been processed */
    private val notificationsChannel = Channel<PgNotification>(capacity = Channel.BUFFERED)
    /** [ReceiveChannel] used to store all server notifications that have not been processed */
    val notifications: ReceiveChannel<PgNotification> = notificationsChannel

    /**
     * Created a log message at the specified [level], applying the [block] to the
     * [KLogger.at][io.github.oshai.kotlinlogging.KLogger.at] method.
     */
    internal inline fun log(level: Level, crossinline block: KLoggingEventBuilder.() -> Unit) {
        logWithResource(logger, level, block)
    }

    /**
     * Receives the next available server message from the underlining connection. Suspends until
     * all [RawMessage] data that is required can be fetched then decodes that [RawMessage] into a
     * [PgMessage] using [PgMessageDecoders.decode].
     */
    suspend fun receiveNextServerMessage(): PgMessage {
        val format = asyncStream.readByte()
        val size = asyncStream.readInt()
        val buffer = asyncStream.readBuffer(size - 4)
        val rawMessage = RawMessage(format = format, size = size, contents = buffer)
        return PgMessageDecoders.decode(rawMessage)
    }

    /**
     * State machine like method that allows the caller to specify a [process] lambda that handles
     * each [PgMessage] received from the server until [Loop.Break] is returned by the lambda.
     * Since unknown errors can arise in [process], the entire loop is wrapped in a try-catch to
     * return a [Result] value rather than throwing an exception. The caller is then responsible
     * for decomposing the [Result]. The only exception that will escape this method is
     * [CancellationException] to allow for regular coroutine cancellation.
     *
     * Some asynchronous messages are not passed forward to [process] because they are not of
     * concern to message processors. These are:
     *
     * - [PgMessage.NoticeResponse]
     * - [PgMessage.NotificationResponse]
     * - [PgMessage.ParameterStatus], should not be received after startup but should be ignored
     * - [PgMessage.BackendKeyData], should not be received after startup but should be ignored
     * - [PgMessage.NegotiateProtocolVersion], should not be received after startup but should be
     * ignored
     *
     * @throws CancellationException when the coroutine scope cancels this coroutine
     */
    suspend inline fun processMessageLoop(process: (PgMessage) -> Loop): Result<Unit> {
        try {
            while (isActive && isConnected) {
                when (val message = receiveNextServerMessage()) {
                    is PgMessage.NoticeResponse -> onNotice(message)
                    is PgMessage.NotificationResponse -> onNotification(message)
                    is PgMessage.ParameterStatus -> onParameterStatus(message)
                    is PgMessage.BackendKeyData -> onBackendKeyData(message)
                    is PgMessage.NegotiateProtocolVersion -> onNegotiateProtocolVersion(message)
                    else -> {
                        when (process(message)) {
                            Loop.Continue, Loop.Noop -> continue
                            Loop.Break -> break
                        }
                    }
                }
            }
        } catch (ex: CancellationException) {
            throw ex
        } catch (ex: Throwable) {
            return Result.failure(ex)
        }
        if (!isActive) {
            return Result.failure(KdbcException(
                "Exited message processing loop because the coroutine scope is no longer active"
            ))
        }
        if (!isConnected) {
            return Result.failure(KdbcException(
                "Exited message processing loop because the underlining connection was closed"
            ))
        }
        return Result.success(Unit)
    }

    /**
     * Similar to [processMessageLoop] but acts as a special case where it ignores all messages
     * except for [T] and [PgMessage.ErrorResponse]. This is helpful when you need to find a
     * specific message but also need to ensure error messages are captured and provided as the
     * failure [Result] option. For example, when starting a `COPY TO` operation,
     * [PgMessage.CopyOutResponse] must be found before continuing to a message processor for
     * [PgMessage.CopyData] messages. You can wait for that message or errors, proceeding if the
     * [Result] is successful.
     */
    suspend inline fun <reified T : PgMessage> waitForOrError(): T {
        while (isActive && isConnected) {
            when (val message = receiveNextServerMessage()) {
                is PgMessage.NoticeResponse -> onNotice(message)
                is PgMessage.NotificationResponse -> onNotification(message)
                is PgMessage.ParameterStatus -> onParameterStatus(message)
                is PgMessage.BackendKeyData -> onBackendKeyData(message)
                is PgMessage.ErrorResponse -> throw GeneralPostgresError(message)
                is PgMessage.NegotiateProtocolVersion -> onNegotiateProtocolVersion(message)
                is T -> return message
                else -> {
                    log(Kdbc.detailedLogging) {
                        this.message =
                            "Ignoring $message since it's not an error or the desired type"
                    }
                }
            }
        }
        throw ExitOfProcessingLoop(T::class.qualifiedName!!)
    }

    /**
     * Similar to [processMessageLoop] but acts as a special case where it ignores all messages
     * except for [PgMessage.NotificationResponse] and [PgMessage.ErrorResponse]. This is only used
     * by a listener to wait for the next notification.
     */
    suspend fun waitForNotificationOrError(): PgNotification {
        while (isConnected) {
            when (val message = receiveNextServerMessage()) {
                is PgMessage.NoticeResponse -> onNotice(message)
                is PgMessage.ParameterStatus -> onParameterStatus(message)
                is PgMessage.BackendKeyData -> onBackendKeyData(message)
                is PgMessage.ErrorResponse -> throw GeneralPostgresError(message)
                is PgMessage.NegotiateProtocolVersion -> onNegotiateProtocolVersion(message)
                is PgMessage.NotificationResponse -> return PgNotification(
                    channelName = message.channelName,
                    payload = message.payload,
                )
                else -> {
                    log(Kdbc.detailedLogging) {
                        this.message = "Ignoring $message since it's not an error or the desired type"
                    }
                }
            }
        }
        throw ExitOfProcessingLoop("PgMessage.NotificationResponse")
    }

    /**
     * Event handler method for [PgMessage.NoticeResponse] messages. Only logs the message details
     */
    private fun onNotice(message: PgMessage.NoticeResponse) {
        log(Kdbc.detailedLogging) {
            this.message = "Notice, message -> $message"
        }
    }

    /**
     * Event handler method for [PgMessage.NotificationResponse] messages. Creates a new
     * [PgNotification] instance and passes that to [notificationsChannel]
     */
    private suspend fun onNotification(message: PgMessage.NotificationResponse) {
        val notification = PgNotification(message.channelName, message.payload)
        log(Kdbc.detailedLogging) {
            this.message = "Notification, message -> $message"
        }
        notificationsChannel.send(notification)
    }

    /**
     * Event handler method for [PgMessage.BackendKeyData] messages. Puts the [message] contents
     * into [backendKeyData].
     */
    private fun onBackendKeyData(message: PgMessage.BackendKeyData) {
        log(Kdbc.detailedLogging) {
            this.message =
                "Got backend key data. Process ID: ${message.processId}, Secret Key: ****"
        }
        backendKeyData = message
    }

    /**
     * Event handler method for [PgMessage.ParameterStatus] messages. Only logs the message details
     */
    private fun onParameterStatus(message: PgMessage.ParameterStatus) {
        log(Kdbc.detailedLogging) {
            this.message = "Parameter Status, $message"
        }
    }

    /**
     * Event handler method for [PgMessage.NegotiateProtocolVersion] messages. Only logs the
     * message details.
     */
    private fun onNegotiateProtocolVersion(message: PgMessage.NegotiateProtocolVersion) {
        log(Kdbc.detailedLogging) {
            this.message = "Server does not support protocol version 3.0. $message"
        }
    }

    /** Write a single [message] to the [PgAsyncStream] using [PgMessageEncoders.encode] */
    suspend inline fun writeToStream(message: PgMessage) {
        writeToBuffer { buffer ->
            PgMessageEncoders.encode(message, buffer)
        }
    }

    /** Write multiple [messages] to the [PgAsyncStream] using [PgMessageEncoders.encode] */
    suspend inline fun writeManyToStream(
        crossinline messages: suspend SequenceScope<PgMessage>.() -> Unit,
    ) {
        writeToBuffer { buffer ->
            for (message in sequence { messages() }) {
                PgMessageEncoders.encode(message, buffer)
            }
        }
    }

    /** Write multiple [messages] to the [PgAsyncStream] using [PgMessageEncoders.encode] */
    suspend inline fun writeManyToStream(vararg messages: PgMessage) {
        writeToBuffer { buffer ->
            for (message in messages) {
                PgMessageEncoders.encode(message, buffer)
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

    /** Returns true if the underlining [asyncStream] is still connected */
    val isConnected: Boolean get() = asyncStream.isConnected

    /**
     * Use the write action, [block], to write zero or more [Byte]s to the [messageSendBuffer]
     * which in turn is written to the [asyncStream]. The [messageSendBuffer] will always be
     * released at the end of this method even if an [Exception] is thrown.
     */
    private suspend inline fun writeToBuffer(crossinline block: suspend (ByteWriteBuffer) -> Unit) {
        try {
            messageSendBuffer.reset()
            block(messageSendBuffer)
            asyncStream.writeBuffer(messageSendBuffer)
        } finally {
            messageSendBuffer.reset()
        }
    }

    /**
     * Utilize the known size of the [M] messages to optimally write a [flow] of messages to the
     * server. This involves collecting the [flow] and packing is as many messages as possible into
     * a single write to the database server.
     */
    suspend fun <M> writeManySized(flow: Flow<M>)
    where
        M : SizedMessage,
        M : PgMessage
    {
        try {
            messageSendBuffer.reset()
            flow.collect {
                if (messageSendBuffer.remaining < it.size) {
                    asyncStream.writeBuffer(messageSendBuffer)
                    messageSendBuffer.reset()
                }
                PgMessageEncoders.encode(it, messageSendBuffer)
            }
            if (messageSendBuffer.position > 0) {
                asyncStream.writeBuffer(messageSendBuffer)
            }
        } finally {
            messageSendBuffer.reset()
        }
    }

    override fun close() {
        messageSendBuffer.close()
        closeChannels()
        if (asyncStream.isConnected) {
            asyncStream.close()
        }
    }

    /**
     * Handle the incoming authentication request with the proper flow of messages. Currently, only
     * clear text password, md5 password and SASL flows are implemented so if the server requests
     * a different authentication flow then an exception will be thrown. This will also throw
     * an exception if the first received message is not a [PgMessage.Authentication] message.
     * In the event the first message is a [PgMessage.ErrorResponse] a [GeneralPostgresError]
     * is thrown.
     *
     * @throws GeneralPostgresError if a [PgMessage.ErrorResponse] is received from the server
     * @throws PgAuthenticationError if a message is received that is not [PgMessage.Authentication]
     * @throws IllegalStateException if the [Authentication] type is not supported
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
                log(Kdbc.detailedLogging) {
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

    private suspend fun requestUpgrade(): Boolean {
        writeToStream(message = PgMessage.SslRequest)
        return when (val response = asyncStream.readByte()) {
            'S'.code.toByte() -> true
            'N'.code.toByte() -> false
            else -> {
                val responseChar = response.toInt().toChar()
                error("Invalid response byte after SSL request. Byte = '$responseChar'")
            }
        }
    }

    private suspend fun upgradeIfNeeded() {
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
                    "TLS connection required by client but server does not accept TLS connection"
                }
            }
        }
        this.asyncStream.upgradeTls(connectOptions.connectionTimeout)
    }

    companion object {
        private const val SEND_BUFFER_SIZE = 4096
        private const val TLS_REJECT_WARNING = "Preferred SSL mode was rejected by server. " +
            "Continuing with non TLS connection"

        /**
         * Create a new TCP connection with the Postgresql database targeted by the
         * [connectOptions] provided. This creates the new [PgAsyncStream] instance.
         *
         * To initiate the Postgresql connection, a [PgMessage.StartupMessage] is sent and the
         * response is handled using [handleAuthFlow]. After the connection has been authenticated
         * The method waits for a [PgMessage.ReadyForQuery] server response. In the case that the
         * server rejects the connection (by sending a [PgMessage.ErrorResponse]) the new
         * [PgAsyncStream] object is closed and an exception is thrown.
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
            asyncStream: AsyncStream,
            connectOptions: PgConnectOptions,
        ): PgAsyncStream {
            asyncStream.connect(timeout = connectOptions.connectionTimeout)
            val stream = PgAsyncStream(
                scope = scope,
                asyncStream = asyncStream,
                connectOptions = connectOptions,
            )
            stream.upgradeIfNeeded()
            val startupMessage = PgMessage.StartupMessage(params = connectOptions.properties)
            stream.writeToStream(message = startupMessage)
            stream.handleAuthFlow()
            stream.waitForOrError<PgMessage.ReadyForQuery>()
            return stream
        }
    }
}
