package com.github.clasicrando.postgresql.connection

import com.github.clasicrando.common.Loop
import com.github.clasicrando.common.atomic.AtomicMutableMap
import com.github.clasicrando.common.connection.Connection
import com.github.clasicrando.common.connectionLogger
import com.github.clasicrando.common.exceptions.UnexpectedTransactionState
import com.github.clasicrando.common.pool.ConnectionPool
import com.github.clasicrando.common.reduceToSingleOrNull
import com.github.clasicrando.common.result.ArrayDataRow
import com.github.clasicrando.common.result.MutableResultSet
import com.github.clasicrando.common.result.QueryResult
import com.github.clasicrando.common.selectLoop
import com.github.clasicrando.postgresql.GeneralPostgresError
import com.github.clasicrando.postgresql.authentication.Authentication
import com.github.clasicrando.postgresql.authentication.saslAuthFlow
import com.github.clasicrando.postgresql.authentication.simplePasswordAuthFlow
import com.github.clasicrando.postgresql.column.PgTypeRegistry
import com.github.clasicrando.postgresql.message.CloseTarget
import com.github.clasicrando.postgresql.message.DescribeTarget
import com.github.clasicrando.postgresql.message.PgMessage
import com.github.clasicrando.postgresql.message.decoders.MessageDecoders
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
import kotlinx.coroutines.CoroutineScope
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

class PgConnection internal constructor(
    internal val connectOptions: PgConnectOptions,
    private val stream: PgStream,
    private val scope: CoroutineScope,
    private val typeRegistry: PgTypeRegistry = PgTypeRegistry(),
    private val encoders: PgMessageEncoders = PgMessageEncoders(
        connectOptions.charset,
        typeRegistry
    ),
    private val decoders: MessageDecoders = MessageDecoders(connectOptions.charset),
    override val connectionId: UUID = UUID.generateUUID(),
) : Connection {

    internal var pool: ConnectionPool<PgConnection>? = null
    private var isAuthenticated = false
    private var backendKeyData: PgMessage.BackendKeyData? = null
    private val _inTransaction: AtomicBoolean = atomic(false)
    override val inTransaction: Boolean get() = _inTransaction.value

    private val canRunQuery = Channel<Unit>(capacity = 1)
    @OptIn(ExperimentalCoroutinesApi::class)
    internal val isWaiting get() = canRunQuery.isEmpty
    private val rowDescriptionChannel = Channel<List<PgRowFieldDescription>>()
    private val dataRowChannel = Channel<PgMessage.DataRow>(capacity = Channel.BUFFERED)
    private val commandCompleteChannel = Channel<PgMessage.CommandComplete>()
    private val releasePreparedStatement: AtomicBoolean = atomic(false)
    private val closeStatementChannel = Channel<Unit>()
    private val queryDoneChannel = Channel<Unit>()
    private val copyInResponseChannel = Channel<Unit>()
    private val copyOutResponseChannel = Channel<Unit>()
    private val copyDataChannel = Channel<PgMessage.CopyData>(capacity = Channel.BUFFERED)
    private val copyDoneChannel = Channel<Unit>()
    private val errorChannel = Channel<Throwable>(capacity = Channel.BUFFERED)
    private val notificationsChannel = Channel<PgNotification>(capacity = Channel.BUFFERED)
    val notifications: ReceiveChannel<PgNotification> get() = notificationsChannel
    private val copyFailed: AtomicBoolean = atomic(false)

    private val preparedStatements: MutableMap<String, PgPreparedStatement> = AtomicMutableMap()

    internal inline fun log(level: Level, crossinline block: KLoggingEventBuilder.() -> Unit) {
        logger.connectionLogger(this, level, block)
    }

    private val messageProcessorJob = scope.launch {
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

    private val initialReadyForQuery: CompletableDeferred<Unit> = CompletableDeferred(
        parent = scope.coroutineContext.job,
    )

    internal suspend fun receiveServerMessage(): PgMessage {
        val rawMessage = stream.receiveMessage()
        return decoders.decode(rawMessage)
    }

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

    private suspend inline fun processServerMessage() {
        when (val message = receiveServerMessage()) {
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

    private fun onNotice(message: PgMessage.NoticeResponse) {
        log(Level.TRACE) {
            this.message = "Notice, message -> {noticeResponse}"
            payload = mapOf("noticeResponse" to message)
        }
    }

    private suspend fun onNotification(message: PgMessage.NotificationResponse) {
        val notification = PgNotification(message.channelName, message.payload)
        log(Level.TRACE) {
            this.message = "Notification, message -> {notification}"
            payload = mapOf("notification" to message)
        }
        notificationsChannel.send(notification)
    }

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

    private fun onBackendKeyData(message: PgMessage.BackendKeyData) {
        log(Level.TRACE) {
            this.message = "Got backend key data. Process ID: {processId}, Secret Key: ****"
            payload = mapOf("payloadId" to message.processId)
        }
        backendKeyData = message
    }

    private fun onParameterStatus(message: PgMessage.ParameterStatus) {
        log(Level.TRACE) {
            this.message = "Parameter Status, {status}"
            payload = mapOf("status" to message)
        }
    }

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

    private suspend fun onRowDescription(message: PgMessage.RowDescription) {
        log(Level.TRACE) {
            this.message = "Received row description -> {desc}"
            payload = mapOf("dec" to message)
        }
        rowDescriptionChannel.send(message.fields)
    }

    private suspend fun onDataRow(message: PgMessage.DataRow) {
        log(Level.TRACE) {
            this.message = "Received row size = {size}"
            payload = mapOf("size" to message.values.size)
        }
        dataRowChannel.send(message)
    }

    private suspend fun onCommandComplete(message: PgMessage.CommandComplete) {
        log(Level.TRACE) {
            this.message = "Received command complete -> {message}"
            payload = mapOf("message" to message)
        }
        commandCompleteChannel.send(message)
    }

    private suspend fun onCloseComplete() {
        log(Level.TRACE) {
            this.message = "Received close complete"
        }
        if (releasePreparedStatement.getAndSet(false)) {
            closeStatementChannel.send(Unit)
        }
    }

    private suspend fun onCopyInResponse(message: PgMessage.CopyInResponse) {
        log(Level.TRACE) {
            this.message = "Received CopyInResponse -> {message}"
            payload = mapOf("message" to message)
        }
        copyInResponseChannel.send(Unit)
    }

    private suspend fun onCopyOutResponse(message: PgMessage.CopyOutResponse) {
        log(Level.TRACE) {
            this.message = "Received CopyOutResponse -> {message}"
            payload = mapOf("message" to message)
        }
        copyOutResponseChannel.send(Unit)
    }

    private suspend fun onCopyData(message: PgMessage.CopyData) {
        log(Level.TRACE) {
            this.message = "Received CopyData, size = {size}"
            payload = mapOf("size" to message.data.size)
        }
        copyDataChannel.send(message)
    }

    private suspend fun onCopyDone() {
        log(Level.TRACE) {
            this.message = "Received CopyDone"
        }
        copyDoneChannel.send(Unit)
    }

    internal suspend inline fun writeToStream(message: PgMessage) {
        val encoder = encoders.encoderFor(message)
        stream.writeMessage { builder ->
            encoder.encode(message, builder)
        }
    }

    private suspend inline fun writeManyToStream(messages: Iterable<PgMessage>) {
        stream.writeMessage { builder ->
            for (message in messages) {
                val encoder = encoders.encoderFor(message)
                encoder.encode(message, builder)
            }
        }
    }

    private suspend inline fun writeManyToStream(vararg messages: PgMessage) {
        writeManyToStream(messages.asIterable())
    }

    private fun checkConnected() {
        check(isConnected) { "Cannot execute queries against a closed connection" }
    }

    private suspend inline fun enableQueryRunning() = canRunQuery.send(Unit)

    private suspend inline fun waitForQueryRunning() = canRunQuery.receive()

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

    private fun collectResults(
        isAutoCommit: Boolean,
        statements: Array<PgPreparedStatement?> = emptyArray(),
    ): Flow<QueryResult> = flow {
        val errors = mutableListOf<Throwable>()
        for ((i, preparedStatement) in statements.withIndex()) {
            var result = MutableResultSet(preparedStatement?.metadata ?: listOf())
            selectLoop {
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
                errorChannel.onReceive {
                    errors.add(it)
                    Loop.Continue
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

    private fun collectResult(
        statement: PgPreparedStatement? = null,
    ): Flow<QueryResult> = flow {
        val errors = mutableListOf<Throwable>()
        var result = MutableResultSet(statement?.metadata ?: listOf())
        selectLoop {
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
            errorChannel.onReceive {
                errors.add(it)
                Loop.Continue
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
        closeStatementChannel.receive()
        preparedStatements.remove(query)
    }

    override suspend fun close() {
        pool?.let {
            if (it.giveBack(this)) {
                return
            }
        }
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

    suspend fun pipelineQueries(vararg queries: Pair<String, List<Any?>>): Flow<QueryResult> {
        return pipelineQueries(queries = queries)
    }

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

    private fun validateCopyQuery(copyStatement: String) {
        require(COPY_CHECK_REGEX.containsMatchIn(copyStatement)) {
            "Invalid copy statement. Statement: $copyStatement"
        }
    }

    suspend fun copyIn(copyInStatement: String, data: Flow<ByteArray>): QueryResult {
        checkConnected()
        waitForQueryRunning()
        validateCopyQuery(copyInStatement)

        log(connectOptions.logSettings.statementLevel) {
            message = STATEMENT_TEMPLATE
            payload = mapOf("query" to copyInStatement)
        }
        writeToStream(PgMessage.Query(copyInStatement))

        copyInResponseChannel.receive()

        val copyInJob = scope.launch {
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

        val error = errors.reduceToSingleOrNull()
        if (error != null) {
            throw error
        }

        return QueryResult(
            completeMessage?.rowCount ?: 0,
            completeMessage?.message ?: "Default copy in complete message",
        )
    }

    suspend fun copyOut(copyOutStatement: String): Flow<ByteArray> {
        checkConnected()
        waitForQueryRunning()
        validateCopyQuery(copyOutStatement)

        log(connectOptions.logSettings.statementLevel) {
            message = STATEMENT_TEMPLATE
            payload = mapOf("query" to copyOutStatement)
        }
        writeToStream(PgMessage.Query(copyOutStatement))

        copyOutResponseChannel.receive()

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

    suspend fun copyIn(
        copyInStatement: String,
        block: suspend FlowCollector<ByteArray>.() -> Unit,
    ): QueryResult {
        return copyIn(copyInStatement, flow(block))
    }

    suspend fun copyInSequence(
        copyInStatement: String,
        data: Sequence<ByteArray>,
    ): QueryResult {
        return copyIn(copyInStatement, data.asFlow())
    }

    suspend fun copyInSequence(
        copyInStatement: String,
        block: suspend SequenceScope<ByteArray>.() -> Unit
    ): QueryResult {
        return copyIn(copyInStatement, sequence(block).asFlow())
    }

    private fun quoteChannelName(channelName: String): String {
        return channelName.replace("\"", "\"\"")
    }

    suspend fun listen(channelName: String) {
        sendQuery("LISTEN \"${quoteChannelName(channelName)}\";")
    }

    suspend fun notify(channelName: String, payload: String) {
        val escapedPayload = payload.replace("'", "''")
        sendQuery("NOTIFY \"${quoteChannelName(channelName)}\", '${escapedPayload}';")
    }

    companion object {
        private val COPY_CHECK_REGEX = Regex("^copy\\s+", RegexOption.IGNORE_CASE)
        private const val STATEMENT_TEMPLATE = "Sending {query}"

        suspend fun connect(connectOptions: PgConnectOptions): PgConnection {
            return PgPoolManager.createConnection(connectOptions)
        }

        internal suspend fun connect(
            connectOptions: PgConnectOptions,
            stream: PgStream,
            scope: CoroutineScope
        ): PgConnection {
            var connection: PgConnection? = null
            try {
                connection = PgConnection(connectOptions, stream, scope)
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