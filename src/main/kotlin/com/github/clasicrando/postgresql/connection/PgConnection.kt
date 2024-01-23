package com.github.clasicrando.postgresql.connection

import com.github.clasicrando.common.Loop
import com.github.clasicrando.common.atomic.AtomicMutableMap
import com.github.clasicrando.common.connection.Connection
import com.github.clasicrando.common.connectionLogger
import com.github.clasicrando.common.exceptions.UnexpectedTransactionState
import com.github.clasicrando.common.pool.ConnectionPool
import com.github.clasicrando.common.quoteIdentifier
import com.github.clasicrando.common.reduceToSingleOrNull
import com.github.clasicrando.common.result.ArrayDataRow
import com.github.clasicrando.common.result.MutableResultSet
import com.github.clasicrando.common.result.QueryResult
import com.github.clasicrando.common.selectLoop
import com.github.clasicrando.common.waitOrError
import com.github.clasicrando.postgresql.GeneralPostgresError
import com.github.clasicrando.postgresql.authentication.Authentication
import com.github.clasicrando.postgresql.authentication.saslAuthFlow
import com.github.clasicrando.postgresql.authentication.simplePasswordAuthFlow
import com.github.clasicrando.postgresql.column.PgTypeRegistry
import com.github.clasicrando.postgresql.copy.CopyStatement
import com.github.clasicrando.postgresql.copy.CopyType
import com.github.clasicrando.postgresql.message.CloseTarget
import com.github.clasicrando.postgresql.message.DescribeTarget
import com.github.clasicrando.postgresql.message.PgMessage
import com.github.clasicrando.postgresql.message.decoders.PgMessageDecoders
import com.github.clasicrando.postgresql.message.encoders.PgMessageEncoders
import com.github.clasicrando.postgresql.notification.PgNotification
import com.github.clasicrando.postgresql.pool.PgPoolManager
import com.github.clasicrando.postgresql.row.PgRowFieldDescription
import com.github.clasicrando.postgresql.statement.PgPreparedStatement
import com.github.clasicrando.postgresql.stream.PgStream
import io.github.oshai.kotlinlogging.KLoggingEventBuilder
import io.github.oshai.kotlinlogging.KotlinLogging
import io.github.oshai.kotlinlogging.Level
import kotlinx.atomicfu.AtomicBoolean
import kotlinx.atomicfu.atomic
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.FlowCollector
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.buffer
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.isActive
import kotlinx.coroutines.job
import kotlinx.coroutines.launch
import kotlinx.coroutines.selects.select
import kotlinx.uuid.UUID
import kotlinx.uuid.generateUUID

private val logger = KotlinLogging.logger {}

/**
 * [Connection] object for a Postgresql database. A new instance cannot be created but rather the
 * [PgConnection.connect] method should be called to receive a new [PgConnection] ready for user
 * usage. This method will use connection pooling behind the scenes as to reduce unnecessary tcp
 * connection creation to the server when an application creates and closes connections frequently.
 */
class PgConnection internal constructor(
    /** Connection options supplied when requesting a new Postgresql connection */
    internal val connectOptions: PgConnectOptions,
    /** Underlining stream of data to and from the database */
    private val stream: PgStream,
    /** Reference to the [ConnectionPool] that owns this [PgConnection] */
    private val pool: ConnectionPool<PgConnection>,
    /**
     * Type registry for connection. Used to decode data rows returned by the server. Defaults to a
     * new instance of [PgTypeRegistry]
     */
    private val typeRegistry: PgTypeRegistry = PgTypeRegistry(),
    /**
     * Container for server message encoders. Used to send messages before they are sent to the
     * server. Defaults to a [PgMessageEncoders] instance with the current connection's charset
     * and [typeRegistry].
     */
    private val encoders: PgMessageEncoders = PgMessageEncoders(
        connectOptions.charset,
        typeRegistry
    ),
    /**
     * Container for server message decoders. Used to parse messages sent from the database server.
     * Defaults to a [PgMessageDecoders] instance with the current connection's charset.
     */
    private val decoders: PgMessageDecoders = PgMessageDecoders(connectOptions.charset),
    override val connectionId: UUID = UUID.generateUUID(),
) : Connection {
    private var isAuthenticated = false
    /** Data sent from the backend during connection initialization */
    private var backendKeyData: PgMessage.BackendKeyData? = null
    private val _inTransaction: AtomicBoolean = atomic(false)
    override val inTransaction: Boolean get() = _inTransaction.value

    /**
     * [Channel] that when containing a single item, signifies the connection is ready for a new
     * query. Do not operate on this directly but rather use the [enableQueryRunning] and
     * [waitForQueryRunning] methods to allow for future calls to this connection to execute
     * queries and wait for the ability to execute a new query, respectively.
     */
    private val canRunQuery = Channel<Unit>(capacity = 1)
    /**
     * Boolean flag indicating that the connection is waiting to run a new query. This is used by
     * the [PgConnectionProvider][com.github.clasicrando.postgresql.pool.PgConnectionProvider] to
     * check if a connection returned to a [ConnectionPool] is in a valid state (i.e. not stuck
     * waiting for a query to finish).
     */
    @OptIn(ExperimentalCoroutinesApi::class)
    internal val isWaiting get() = canRunQuery.isEmpty
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
    /** Thread safe flag indicating that a user requested a prepared statement to be released */
    private val releasePreparedStatement: AtomicBoolean = atomic(false)
    /**
     * [Channel] used to notify a request to close a prepared statement that the close was
     * completed
     */
    private val closeStatementChannel = Channel<Unit>()
    /** [Channel] used to notify a query request that the query is now complete */
    private val queryDoneChannel = Channel<Unit>()
    /** [Channel] used to notify a [copyIn] request that the server is now ready for data rows */
    private val copyInResponseChannel = Channel<Unit>()
    /** [Channel] used to notify a [copyOut] request that the server will now send data rows */
    private val copyOutResponseChannel = Channel<Unit>()
    /**
     * [Channel] used to pass [PgMessage.CopyData] rows sent by the server to the [copyOut]
     * executor.
     */
    private val copyDataChannel = Channel<PgMessage.CopyData>(capacity = Channel.BUFFERED)
    /** [Channel] used to notify a [copyOut] request that the server is done sending data rows */
    private val copyDoneChannel = Channel<Unit>()
    /**
     * [Channel] used to pass [GeneralPostgresError] instances to command executors to notify them
     * of a [PgMessage.ErrorResponse] message from the server.
     */
    private val errorChannel = Channel<GeneralPostgresError>(capacity = Channel.BUFFERED)
    /** [Channel] used to store all server notifications that have not been processed */
    private val notificationsChannel = Channel<PgNotification>(capacity = Channel.BUFFERED)
    /**
     * [ReceiveChannel] for [PgNotification]s received by the server. Although this method is
     * available to anyone who has a reference to this [PgConnection], you should only have one
     * coroutine that consumes this channel since messages are not duplicated for multiple
     * receivers so messages are consumed fairly in a first come, first server basis. The
     * recommended practice would then be to spawn a coroutine to read this channel until it closes
     * or consume this channel as a flow and dispatch each [PgNotification] to the desired
     * consumers, copying the [PgNotification] if needed.
     */
    val notifications: ReceiveChannel<PgNotification> get() = notificationsChannel
    /**
     * Thread safe flag indicating that the client requested a copy fail and the resulting server
     * error message should not throw an exception when received.
     */
    private val copyFailed: AtomicBoolean = atomic(false)
    /**
     * Thread safe cache of [PgPreparedStatement] where the key is the query that initiated the
     * prepared statement.
     *
     * TODO implement a cap to the cache so we don't hold too many prepared statements
     */
    private val preparedStatements: MutableMap<String, PgPreparedStatement> = AtomicMutableMap()

    /**
     * Created a log message at the specified [level], applying the [block] to the
     * [KLogger.at][io.github.oshai.kotlinlogging.KLogger.at] method.
     */
    internal inline fun log(level: Level, crossinline block: KLoggingEventBuilder.() -> Unit) {
        logger.connectionLogger(this, level, block)
    }

    /**
     * Coroutine launched to process server messages received by the [stream]. This also
     * initializes the connection by sending the [PgMessage.StartupMessage] and handling the server
     * authentication.
     */
    private val messageProcessorJob = pool.launch {
        var cause: Throwable? = null
        writeToStream(PgMessage.StartupMessage(params = connectOptions.properties))
        try {
            handleAuthFlow()
            while (isActive && stream.isActive) {
                processServerMessage()
            }
        } catch(cancel: CancellationException) {
            cause = cancel
            throw cancel
        } catch (ex: Throwable) {
            cause = ex
            log(Level.ERROR) {
                message = "Error within server message processor"
                this.cause = ex
            }
            initialReadyForQuery.completeExceptionally(ex)
        } finally {
            closeChannels(cause)
            log(Level.TRACE) {
                message = "Server Message Processor is closing"
            }
        }
    }

    /**
     * Await-able future that completes when the connection initialization and authentication is
     * complete.
     */
    private val initialReadyForQuery: CompletableDeferred<Unit> = CompletableDeferred(
        parent = pool.coroutineContext.job,
    )

    /**
     * Receive the next [RawMessage][com.github.clasicrando.postgresql.stream.RawMessage] and
     * decode the contents using this connections [decoders]
     */
    internal suspend fun receiveServerMessage(): PgMessage {
        val rawMessage = stream.receiveMessage()
        return decoders.decode(rawMessage)
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
        canRunQuery.close(cause = throwable)
        errorChannel.close(cause = throwable)
    }

    /** Delegate the next received server message to the proper event handler method */
    private suspend inline fun processServerMessage() {
        val message = receiveServerMessage()
        when (message) {
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

    /**
     * Handle the incoming authentication request with the proper flow of messages. Currently, only
     * clear text password, md5 password and SASL flows are implemented so if the server requests
     * and different authentication flow then an exception will be thrown. This will also throw
     * an exception if the first received message is not a [PgMessage.Authentication] message.
     * In the event the first message is a [PgMessage.ErrorResponse] the [onErrorMessage] handler
     * is invoked which signals the [PgConnection] initialization method that the connection should
     * be disposed.
     */
    private suspend fun handleAuthFlow() {
        val message = receiveServerMessage()
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
     * message is sent to [commandCompleteChannel] to notify the [copyIn] caller that the
     * `COPY FROM` call was failed.
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
     * Event handler method for [PgMessage.ReadyForQuery] messages. Attempts to complete the
     * [initialReadyForQuery] future and if it does, [enableQueryRunning] is called and no message
     * is sent through [queryDoneChannel]. If the future has already been completed, an empty
     * message is sent through [queryDoneChannel].
     */
    private suspend fun onReadyForQuery(message: PgMessage.ReadyForQuery) {
        log(Level.TRACE) {
            this.message = "Connection ready for query. Transaction Status: {status}"
            payload = mapOf("status" to message.transactionStatus)
        }
        if (initialReadyForQuery.complete(Unit)) {
            enableQueryRunning()
            log(Level.TRACE) {
                this.message = "Connection is now initialized and ready for queries"
            }
            return
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
     * Event handler method for [PgMessage.CopyInResponse] messages. Notifies the [copyIn] caller
     * that the server is ready to proceed.
     */
    private suspend fun onCopyInResponse(message: PgMessage.CopyInResponse) {
        log(Level.TRACE) {
            this.message = "Received CopyInResponse -> {message}"
            payload = mapOf("message" to message)
        }
        copyInResponseChannel.send(Unit)
    }

    /**
     * Event handler method for [PgMessage.CopyInResponse] messages. Notifies the [copyOut] caller
     * that the server is ready to proceed.
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
     * Event handler method for [PgMessage.CopyInResponse] messages. Notifies the [copyOut] caller
     * that the server is done sending data.
     */
    private suspend fun onCopyDone() {
        log(Level.TRACE) {
            this.message = "Received CopyDone"
        }
        copyDoneChannel.send(Unit)
    }

    /** Write a single [message] to the [stream] using the appropriate derived encoder. */
    internal suspend inline fun writeToStream(message: PgMessage) {
        val encoder = encoders.encoderFor(message)
        stream.writeMessage { builder ->
            encoder.encode(message, builder)
        }
    }

    /** Write multiple [messages] to the [stream] using the appropriate derived encoders. */
    private suspend inline fun writeManyToStream(messages: Iterable<PgMessage>) {
        stream.writeMessage { builder ->
            for (message in messages) {
                val encoder = encoders.encoderFor(message)
                encoder.encode(message, builder)
            }
        }
    }

    /** Write multiple [messages] to the [stream] using the appropriate derived encoders. */
    private suspend inline fun writeManyToStream(vararg messages: PgMessage) {
        writeManyToStream(messages.asIterable())
    }

    /**
     * Verify that the connection is currently active. Throws an [IllegalStateException] is the
     * underlining connection is no longer active. This should be performed before each call to for
     * the [PgConnection] to interact with the server.
     */
    private fun checkConnected() {
        check(isConnected) { "Cannot execute queries against a closed connection" }
    }

    /** Send a message to the [canRunQuery] channel to enable future users to execute query */
    private suspend inline fun enableQueryRunning() = canRunQuery.send(Unit)

    /**
     * Suspend until there is a message in the [canRunQuery] channel. This signifies that no other
     * user is currently running a query and the server has notified the client that it is ready
     * for another query.
     */
    private suspend inline fun waitForQueryRunning() = canRunQuery.receive()

    /** Utility method to add a new [dataRow] to the specified [resultSet] */
    private fun addDataRow(resultSet: MutableResultSet, dataRow: PgMessage.DataRow) {
        val fields: Array<Any?> = Array(dataRow.values.size) { i ->
            dataRow.values[i]?.let {
                val columnType = resultSet.columnMapping[i]
                typeRegistry.decode(columnType, it, connectOptions.charset)
            }
        }
        resultSet.addRow(ArrayDataRow(columnMapping = resultSet.columnMap, values = fields))
    }

    override val isConnected: Boolean get() = stream.isActive && messageProcessorJob.isActive

    override suspend fun begin() {
        try {
            sendQuery("BEGIN;")
            if (_inTransaction.compareAndSet(expect = false, update = true)) {
                throw UnexpectedTransactionState(inTransaction = true)
            }
        } catch (ex: Throwable) {
            if (_inTransaction.getAndSet(false)) {
                try {
                    rollback()
                } catch (ex2: Throwable) {
                    log(Level.ERROR) {
                        message = "Error while trying to rollback. BEGIN called while in transaction"
                        cause = ex2
                    }
                }
            }
            throw ex
        }
    }

    override suspend fun commit() {
        try {
            sendQuery("COMMIT;")
        } finally {
            if (!_inTransaction.getAndSet(false)) {
                log(Level.ERROR) {
                    this.message = "Attempted to COMMIT a connection not within a transaction"
                }
            }
        }
    }

    override suspend fun rollback() {
        try {
            sendQuery("ROLLBACK;")
        } finally {
            if (!_inTransaction.getAndSet(false)) {
                log(Level.ERROR) {
                    this.message = "Attempted to ROLLBACK a connection not within a transaction"
                }
            }
        }
    }

    /**
     * Collect all [QueryResult]s for the executed [statements] as a buffered [Flow].
     *
     * This iterates over all [statements], updating the [PgPreparedStatement] with the received
     * metadata (if any), collecting each row received, and exiting the current iteration once a
     * query done message is received. The collector only emits a [QueryResult] for the iteration
     * if a [PgMessage.CommandComplete] message is received.
     *
     * If an error message is received during result collection and [isAutoCommit] is false
     * (meaning there is no implicit commit between each statement), the previously received query
     * done message was signifying the end of all results and the [statements] iteration is
     * terminated. If [isAutoCommit] was true, then each result is processed with all errors also
     * collected into a list for evaluation later.
     *
     * Once the iteration over [statements] is completed (successfully or with errors), the
     * collection of errors is checked and aggregated into one [Throwable] (if any) and thrown.
     * Otherwise, the [flow] exits with all [QueryResult]s yielded.
     */
    private fun collectResults(
        isAutoCommit: Boolean,
        statements: Array<PgPreparedStatement?> = emptyArray(),
    ): Flow<QueryResult> = flow {
        val errors = mutableListOf<Throwable>()
        for ((i, preparedStatement) in statements.withIndex()) {
            var result = MutableResultSet(preparedStatement?.metadata ?: listOf())
            selectLoop {
                errorChannel.onReceive {
                    errors.add(it)
                    Loop.Continue
                }
                rowDescriptionChannel.onReceive {
                    preparedStatement?.metadata = it
                    result = MutableResultSet(it)
                    Loop.Continue
                }
                dataRowChannel.onReceive {
                    addDataRow(result, it)
                    Loop.Continue
                }
                commandCompleteChannel.onReceive {
                    emit(QueryResult(it.rowCount, it.message, result))
                    Loop.Continue
                }
                queryDoneChannel.onReceive {
                    if (i == statements.size - 1) {
                        enableQueryRunning()
                    }
                    Loop.Break
                }
            }
            if (errors.isNotEmpty() && !isAutoCommit) {
                enableQueryRunning()
                break
            }
        }

        val error = errors.reduceToSingleOrNull() ?: return@flow
        log(Level.ERROR) {
            message = "Error during single query execution"
            cause = error
        }
        throw error
    }.buffer()

    /**
     * Collect all [QueryResult]s for the query as a buffered [Flow].
     *
     * This allows for multiple results sets from a single query to be collected by continuously
     * looping over a [select], exiting only once a query done message is received. There will only
     * ever be multiple results if the [statement] parameter is null (i.e. it's a simple query).
     *
     * Within the [select], it received any sent:
     * - error -> packing into error collection
     * - row description -> updating the [statement] if any and creating a new [MutableResultSet]
     * - data row -> pack the row into the current [MutableResultSet]
     * - command complete -> emitting a new [QueryResult] from the [flow]
     * - query done -> enabling others to query against this connection and exiting the
     * [selectLoop]
     *
     * After the query collection the collection of errors is checked and aggregated into one
     * [Throwable] (if any) and thrown. Otherwise, the [flow] exits with all [QueryResult]s
     * yielded.
     */
    private fun collectResult(
        statement: PgPreparedStatement? = null,
    ): Flow<QueryResult> = flow {
        val errors = mutableListOf<Throwable>()
        var result = MutableResultSet(statement?.metadata ?: listOf())
        selectLoop {
            errorChannel.onReceive {
                errors.add(it)
                Loop.Continue
            }
            rowDescriptionChannel.onReceive {
                statement?.metadata = it
                result = MutableResultSet(it)
                Loop.Continue
            }
            dataRowChannel.onReceive {
                addDataRow(result, it)
                Loop.Continue
            }
            commandCompleteChannel.onReceive {
                emit(QueryResult(it.rowCount, it.message, result))
                Loop.Continue
            }
            queryDoneChannel.onReceive {
                enableQueryRunning()
                Loop.Break
            }
        }

        val error = errors.reduceToSingleOrNull() ?: return@flow
        log(Level.ERROR) {
            message = "Error during single query execution"
            cause = error
        }
        throw error
    }.buffer()

    override suspend fun sendQueryFlow(query: String): Flow<QueryResult> {
        require(query.isNotBlank()) { "Cannot send an empty query" }
        checkConnected()
        waitForQueryRunning()
        logger.connectionLogger(
            this@PgConnection,
            connectOptions.logSettings.statementLevel,
        ) {
            message = STATEMENT_TEMPLATE
            payload = mapOf("query" to query)
        }
        writeToStream(PgMessage.Query(query))

        return collectResult()
    }

    /**
     * Send all required messages to the server for prepared statement execution.
     *
     * To start, the cache is checked or pushed to for the supplied [query]. After that, all
     * messages to send are as follows:
     * - If the query has not already been prepared, [PgMessage.Parse] is sent
     * - [PgMessage.Bind] with statement name + current parameters
     * - If the statement has not received metadata, [PgMessage.Describe] is sent
     * - [PgMessage.Execute] with the statement name
     * - [PgMessage.Close] with the statement name
     * - If [sendSync] is true, [PgMessage.Sync] is sent
     *
     * TODO don't send a close message if the cache is not full, also add logic to remove old items from cache
     */
    private suspend fun sendPreparedStatementMessage(
        query: String,
        parameters: List<Any?>,
        sendSync: Boolean = true,
    ): PgPreparedStatement {
        val statement = preparedStatements.getOrPut(query) {
            PgPreparedStatement(query)
        }

        require(statement.paramCount == parameters.size) {
            enableQueryRunning()
            """
            Query does not have the correct number of parameters.
            
            ${query.replaceIndent("            ")}
            
            Expected ${statement.paramCount}, got ${parameters.size}
            """.trimIndent()
        }

        val messages = buildList {
            if (!statement.prepared) {
                val parseMessage = PgMessage.Parse(
                    preparedStatementName = statement.statementName,
                    query = query,
                    parameters = parameters,
                )
                statement.prepared = true
                add(parseMessage)
            }
            val bindMessage = PgMessage.Bind(
                portal = statement.statementName,
                statementName = statement.statementName,
                parameters = parameters,
            )
            add(bindMessage)
            if (statement.metadata.isEmpty()) {
                val describeMessage = PgMessage.Describe(
                    target = DescribeTarget.Portal,
                    name = statement.statementName,
                )
                add(describeMessage)
            }
            val executeMessage = PgMessage.Execute(
                portalName = statement.statementName,
                maxRowCount = 0,
            )
            add(executeMessage)
            val closePortalMessage = PgMessage.Close(
                target = CloseTarget.Portal,
                targetName = statement.statementName,
            )
            add(closePortalMessage)
            if (sendSync) {
                add(PgMessage.Sync)
            }
        }
        logger.connectionLogger(
            this@PgConnection,
            connectOptions.logSettings.statementLevel,
        ) {
            message = STATEMENT_TEMPLATE
            payload = mapOf("query" to query)
        }
        writeManyToStream(messages)
        return statement
    }

    override suspend fun sendPreparedStatementFlow(
        query: String,
        parameters: List<Any?>,
    ): Flow<QueryResult> {
        require(query.isNotBlank()) { "Cannot send an empty query" }
        checkConnected()
        waitForQueryRunning()
        val statement = sendPreparedStatementMessage(query, parameters)
        return collectResult(statement = statement)
    }

    override suspend fun releasePreparedStatement(query: String) {
        waitForQueryRunning()
        if (!releasePreparedStatement.compareAndSet(expect = false, update = true)) {
            error("Cannot release a prepared statement while releasing another")
        }

        val statement = preparedStatements[query]
        if (statement == null) {
            log(Level.WARN) {
                message = "query supplied did not match a stored prepared statement"
                payload = mapOf("query" to query)
            }
            return
        }
        val closeMessage = PgMessage.Close(
            target = CloseTarget.PreparedStatement,
            targetName = statement.statementName,
        )
        writeManyToStream(closeMessage, PgMessage.Sync)
        waitOrError(errorChannel, closeStatementChannel)
        preparedStatements.remove(query)
    }

    /**
     * Dispose of all internal connection resources while also sending a [PgMessage.Terminate] so
     * the database server is alerted to the closure.
     */
    internal suspend fun dispose() {
        try {
            if (stream.isActive) {
                writeToStream(PgMessage.Terminate)
                log(Level.INFO) {
                    this.message = "Successfully sent termination message"
                }
            }
        } catch (ex: Throwable) {
            log(Level.WARN) {
                this.message = "Error sending terminate message"
                cause = ex
            }
        } finally {
            stream.close()
        }
        if (messageProcessorJob.isActive) {
            messageProcessorJob.cancelAndJoin()
        }
    }

    override suspend fun close() {
        pool.giveBack(this)
    }

    /**
     * Allows for vararg specification of prepared statements using the default parameters of
     * [pipelineQueries]. See the other method doc for more information.
     */
    suspend fun pipelineQueries(vararg queries: Pair<String, List<Any?>>): Flow<QueryResult> {
        return pipelineQueries(queries = queries)
    }

    /**
     * Execute the prepared [queries] provided using the Postgresql query pipelining method. This
     * allows for sending multiple prepared queries at once to the server, so you do not need to
     * wait for previous queries to complete to request another result.
     *
     * ```
     * Regular Pipelined
     * | Client         | Server          |    | Client         | Server          |
     * |----------------|-----------------|    |----------------|-----------------|
     * | send query 1   |                 |    | send query 1   |                 |
     * |                | process query 1 |    | send query 2   | process query 1 |
     * | receive rows 1 |                 |    | send query 3   | process query 2 |
     * | send query 2   |                 |    | receive rows 1 | process query 3 |
     * |                | process query 2 |    | receive rows 2 |                 |
     * | receive rows 2 |                 |    | receive rows 3 |                 |
     * | send query 3   |                 |
     * |                | process query 3 |
     * | receive rows 3 |                 |
     * ```
     *
     * This can reduce server round trips, however there is one limitation to this client's
     * implementation of query pipelining. Currently, the client takes an all or nothing approach
     * where sync messages are sent after each query (instructing an autocommit by the server
     * unless already in an open transaction) by default. To override this behaviour, allowing all
     * statements after the failed one to be skipped and all previous statement changes to be
     * rolled back, change the [syncAll] parameter to false.
     *
     * If you are sure each one of your statements do not impact each other and can be handled in
     * separate transactions, keep the [syncAll] as default and catch exception thrown during
     * query execution. Alternatively, you can also manually begin a transaction using [begin]
     * and handle the transaction state of your connection yourself. In that case, any sync message
     * sent to the server does not cause implicit transactional behaviour.
     *
     * If you are unsure of how this works or what the implications of pipelining has on your
     * database, you should opt to either send multiple statements in separate calls to
     * [sendPreparedStatementFlow] or package your queries into a stored procedure.
     */
    suspend fun pipelineQueries(
        syncAll: Boolean = true,
        queries: Array<out Pair<String, List<Any?>>>,
    ): Flow<QueryResult> {
        waitForQueryRunning()
        val statements = Array<PgPreparedStatement?>(queries.size) { i ->
            val (queryText, queryParams) = queries[i]
            sendPreparedStatementMessage(
                queryText,
                queryParams,
                sendSync = syncAll || i == queries.size - 1,
            )
        }
        return collectResults(syncAll, statements)
    }

    /**
     * Execute a `COPY FROM` command using the options supplied in the [copyInStatement] and feed
     * the [data] provided as a [Flow] to the server. Since the server will parse the bytes
     * supplied as a continuous flow of data rather than records, each item in the flow does not
     * need to represent a record but, it's usually convenient to parse data as records, convert to
     * a [ByteArray] and feed that through the flow.
     *
     * If the server sends an error message during or at completion of streaming the copy [data],
     * the message will be captured and thrown after completing the COPY process and the connection
     * with the server reverts to regular queries.
     */
    suspend fun copyIn(copyInStatement: CopyStatement, data: Flow<ByteArray>): QueryResult {
        checkConnected()
        waitForQueryRunning()

        val copyQuery = copyInStatement.toStatement(CopyType.From)
        log(connectOptions.logSettings.statementLevel) {
            message = STATEMENT_TEMPLATE
            payload = mapOf("query" to copyQuery)
        }
        writeToStream(PgMessage.Query(copyQuery))

        waitOrError(errorChannel, copyInResponseChannel)

        val copyInJob = pool.launch {
            try {
                data.collect {
                    writeToStream(PgMessage.CopyData(it))
                }
                writeToStream(PgMessage.CopyDone)
            } catch (ex: Throwable) {
                if (stream.isActive) {
                    copyFailed.value = true
                    writeToStream(PgMessage.CopyFail("Exception collecting data\nError:\n$ex"))
                }
            }
        }

        var completeMessage: PgMessage.CommandComplete? = null
        val errors = mutableListOf<Throwable>()
        try {
            selectLoop {
                errorChannel.onReceive {
                    if (copyInJob.isActive) {
                        copyInJob.cancel()
                    }
                    copyInJob.join()
                    log(Level.ERROR) {
                        message = "Error during copy in operation"
                        cause = it
                    }
                    errors.add(it)
                    Loop.Continue
                }
                commandCompleteChannel.onReceive {
                    completeMessage = it
                    Loop.Continue
                }
                queryDoneChannel.onReceive {
                    copyInJob.join()
                    enableQueryRunning()
                    Loop.Break
                }
            }
        } catch (ex: Throwable) {
            copyInJob.cancelAndJoin()
            errors.add(ex)
        }

        val error = errors.reduceToSingleOrNull()
        if (error != null) {
            throw error
        }

        return QueryResult(
            completeMessage?.rowCount ?: 0,
            completeMessage?.message ?: "Default copy in complete message",
        )
    }

    /**
     * Execute a `COPY TO` command using the options supplied in the [copyOutStatement], reading
     * each `CopyData` server response message and passing the data through the returned buffered
     * [Flow]. The returned [Flow]'s buffer matches the [Channel.BUFFERED] behaviour so if you
     * want to avoid suspending the server message processor, you should always try to process each
     * item as soon as possible or collect the elements into a [List].
     */
    suspend fun copyOut(copyOutStatement: CopyStatement): Flow<ByteArray> {
        checkConnected()
        waitForQueryRunning()

        val copyQuery = copyOutStatement.toStatement(CopyType.To)
        log(connectOptions.logSettings.statementLevel) {
            message = STATEMENT_TEMPLATE
            payload = mapOf("query" to copyQuery)
        }
        writeToStream(PgMessage.Query(copyQuery))

        waitOrError(errorChannel, copyOutResponseChannel)
        return flow {
            selectLoop {
                errorChannel.onReceive {
                    enableQueryRunning()
                    throw it
                }
                copyDataChannel.onReceive {
                    emit(it.data)
                    Loop.Continue
                }
                copyDoneChannel.onReceive {
                    Loop.Continue
                }
                commandCompleteChannel.onReceive {
                    Loop.Continue
                }
                queryDoneChannel.onReceive {
                    enableQueryRunning()
                    Loop.Break
                }
            }
        }.buffer()
    }

    /**
     * Allows for specifying a [FlowCollector] for the `COPY FROM` operation. This method simply
     * uses the [block] in a [flow] builder, forwarding that [Flow] to the [copyIn] method.
     *
     * @see [copyIn]
     */
    suspend fun copyIn(
        copyInStatement: CopyStatement,
        block: suspend FlowCollector<ByteArray>.() -> Unit,
    ): QueryResult {
        return copyIn(copyInStatement, flow(block))
    }

    /**
     * Allows for providing a synchronous [Sequence] to [copyIn] by converting the [data] into a
     * [Flow].
     *
     * @see [copyIn]
     */
    suspend fun copyInSequence(
        copyInStatement: CopyStatement,
        data: Sequence<ByteArray>,
    ): QueryResult {
        return copyIn(copyInStatement, data.asFlow())
    }

    /**
     * Allows for specifying a [SequenceScope] for the `COPY FROM` operation. This method simply
     * uses the [block] in a [sequence] builder, converting that [Sequence] to a [Flow] and passing
     * that to the [copyIn] method.
     *
     * @see [copyIn]
     */
    suspend fun copyInSequence(
        copyInStatement: CopyStatement,
        block: suspend SequenceScope<ByteArray>.() -> Unit
    ): QueryResult {
        return copyIn(copyInStatement, sequence(block).asFlow())
    }

    /**
     * Execute a `LISTEN` command for the specified [channelName]. Allows this connection to
     * receive notifications sent to this connection's current database. All received messages
     * are accessible from the [notifications] [ReceiveChannel].
     */
    suspend fun listen(channelName: String) {
        val query = "LISTEN ${channelName.quoteIdentifier()};"
        sendQuery(query)
    }

    /**
     * Execute a `NOTIFY` command for the specified [channelName] with the supplied [payload]. This
     * sends a notification to any connection connected to this connection's current database.
     */
    suspend fun notify(channelName: String, payload: String) {
        val escapedPayload = payload.replace("'", "''")
        sendQuery("NOTIFY ${channelName.quoteIdentifier()}, '${escapedPayload}';")
    }

    companion object {
        private const val STATEMENT_TEMPLATE = "Sending {query}"

        /**
         * Create a new [PgConnection] (or reuse an existing connection if any are available) using
         * the supplied [PgConnectOptions].
         */
        suspend fun connect(connectOptions: PgConnectOptions): PgConnection {
            return PgPoolManager.createConnection(connectOptions)
        }

        /**
         * Create a new [PgConnection] instance using the supplied [connectOptions], [stream] and
         * [pool] (the pool that owns this connection). This creates the new [PgConnection]
         * instance which in turn starts the background [messageProcessorJob] that received
         * incoming server messages. To start this job, a [PgMessage.StartupMessage] is sent and
         * the resulting response is handled using [handleAuthFlow]. After the connection has been
         * authenticated the [initialReadyForQuery] future is completed alerting this method that
         * the new [PgConnection] is ready for a user to interact with it. In the case that the
         * server rejects the connection (by sending a [PgMessage.ErrorResponse]) the new
         * [PgConnection] object is closed and an exception is thrown.
         *
         * As a final step to preparing the connection for user interaction, the [typeRegistry] is
         * updated with any custom types required using the [PgTypeRegistry.finalizeTypes] method.
         * This may fail and will also cause the connection to be closed and an exception thrown.
         */
        internal suspend fun connect(
            connectOptions: PgConnectOptions,
            stream: PgStream,
            pool: ConnectionPool<PgConnection>,
        ): PgConnection {
            var connection: PgConnection? = null
            try {
                connection = PgConnection(connectOptions, stream, pool)
                select {
                    connection.initialReadyForQuery.onAwait {
                        connection.log(Level.TRACE) {
                            message = "Received initial ReadyForQuery message"
                        }
                    }
                    connection.errorChannel.onReceive {
                        throw it
                    }
                }
                if (!connection.isConnected) {
                    error("Could not initialize connection")
                }
                connection.typeRegistry.finalizeTypes(connection)
                return connection
            } catch (ex: Throwable) {
                try {
                    connection?.close()
                } catch (ex2: Throwable) {
                    ex.addSuppressed(ex2)
                }
                throw ex
            }
        }
    }
}