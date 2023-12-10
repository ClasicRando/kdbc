package com.github.clasicrando.postgresql

import com.github.clasicrando.common.Loop
import com.github.clasicrando.common.atomic.AtomicMutableMap
import com.github.clasicrando.common.connectionLogger
import com.github.clasicrando.common.exceptions.ConnectionNotRunningQuery
import com.github.clasicrando.common.exceptions.ConnectionRunningQuery
import com.github.clasicrando.common.exceptions.UnexpectedTransactionState
import com.github.clasicrando.common.pool.ConnectionPool
import com.github.clasicrando.common.pool.PoolConnection
import com.github.clasicrando.common.result.ArrayDataRow
import com.github.clasicrando.common.result.MutableResultSet
import com.github.clasicrando.common.result.QueryResult
import com.github.clasicrando.common.selectLoop
import com.github.clasicrando.postgresql.authentication.Authentication
import com.github.clasicrando.postgresql.authentication.saslAuthFlow
import com.github.clasicrando.postgresql.authentication.simplePasswordAuthFlow
import com.github.clasicrando.postgresql.column.PgTypeRegistry
import com.github.clasicrando.postgresql.message.CloseTarget
import com.github.clasicrando.postgresql.message.DescribeTarget
import com.github.clasicrando.postgresql.message.PgMessage
import com.github.clasicrando.postgresql.message.decoders.MessageDecoders
import com.github.clasicrando.postgresql.message.encoders.MessageEncoders
import com.github.clasicrando.postgresql.row.PgRowFieldDescription
import com.github.clasicrando.postgresql.stream.PgStream
import io.github.oshai.kotlinlogging.KLoggingEventBuilder
import io.github.oshai.kotlinlogging.KotlinLogging
import io.github.oshai.kotlinlogging.Level
import io.ktor.utils.io.charsets.Charset
import kotlinx.atomicfu.AtomicBoolean
import kotlinx.atomicfu.AtomicRef
import kotlinx.atomicfu.atomic
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.isActive
import kotlinx.coroutines.job
import kotlinx.coroutines.launch
import kotlinx.uuid.UUID
import kotlinx.uuid.generateUUID

private val logger = KotlinLogging.logger {}

class PgConnectionImpl private constructor(
    internal val configuration: PgConnectOptions,
    internal val charset: Charset,
    private val stream: PgStream,
    private val scope: CoroutineScope,
    private val typeRegistry: PgTypeRegistry = PgTypeRegistry(),
    private val encoders: MessageEncoders = MessageEncoders(charset, typeRegistry),
    private val decoders: MessageDecoders = MessageDecoders(charset),
    override val connectionId: UUID = UUID.generateUUID(),
    override var pool: ConnectionPool? = null,
) : PgConnection, PoolConnection {

    private var isAuthenticated = false
    private var backendKeyData: PgMessage.BackendKeyData? = null
    private val _inTransaction: AtomicBoolean = atomic(false)
    override val inTransaction: Boolean get() = _inTransaction.value

    private val queryRunning: AtomicBoolean = atomic(true)
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
    private val copyFailed: AtomicBoolean = atomic(false)

    private val preparedStatements: MutableMap<String, PgPreparedStatement> = AtomicMutableMap()
    private val currentPreparedStatement: AtomicRef<PgPreparedStatement?> = atomic(null)
    
    internal fun log(level: Level, block: KLoggingEventBuilder.() -> Unit) {
        logger.connectionLogger(this, level, block)
    }

    private val messageProcessorJob = scope.launch {
        writeToStream(PgMessage.StartupMessage(params = configuration.properties))
        try {
            handleAuthFlow()
            while (isActive && stream.isActive) {
                processServerMessage()
            }
        } catch(cancel: CancellationException) {
            throw cancel
        } catch (ex: Throwable) {
            log(Level.ERROR) {
                message = "Error within server message processor"
                cause = ex
            }
            closeChannels(ex)
        } finally {
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

    private fun closeChannels(throwable: Throwable) {
        rowDescriptionChannel.close(cause = throwable)
        dataRowChannel.close(cause = throwable)
        commandCompleteChannel.close(cause = throwable)
        queryDoneChannel.close(cause = throwable)
        closeStatementChannel.close(cause = throwable)
        copyOutResponseChannel.close(cause = throwable)
        copyInResponseChannel.close(cause = throwable)
        copyDataChannel.close(cause = throwable)
        copyDoneChannel.close(cause = throwable)
    }

    private suspend fun processServerMessage() {
        when (val message = receiveServerMessage()) {
            is PgMessage.ErrorResponse -> onErrorMessage(message)
            is PgMessage.NoticeResponse -> onNotice(message)
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
                logger.connectionLogger(this, Level.TRACE) {
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
                logger.connectionLogger(this, Level.TRACE) {
                    this.message = "Successfully logged in to database"
                }
                isAuthenticated = true
            }
            Authentication.CleartextPassword -> {
                isAuthenticated = this.simplePasswordAuthFlow(
                    configuration.username,
                    configuration.password ?: error("Password must be provided"),
                )
            }
            is Authentication.Md5Password -> {
                isAuthenticated = this.simplePasswordAuthFlow(
                    configuration.username,
                    configuration.password ?: error("Password must be provided"),
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
        logger.connectionLogger(this, Level.TRACE) { 
            this.message = "Notice, message -> {noticeResponse}"
            payload = mapOf("noticeResponse" to message)
        }
    }

    private suspend fun onErrorMessage(message: PgMessage.ErrorResponse) {
        if (copyFailed.getAndSet(false)) {
            logger.connectionLogger(this, Level.WARN) {
                this.message = "CopyIn was failed by client -> {message}"
                payload = mapOf("message" to message)
            }
            val commandComplete = PgMessage.CommandComplete(0, "CopyIn failed by client")
            commandCompleteChannel.send(commandComplete)
            return
        }
        logger.connectionLogger(this, Level.ERROR) {
            this.message = "Error, message -> {errorResponse}"
            payload = mapOf("errorResponse" to message)
        }
        val throwable = GeneralPostgresError(message)
        stream.close()
        closeChannels(throwable)
        if (!initialReadyForQuery.isCompleted) {
            initialReadyForQuery.complete(Unit)
        }
    }

    private fun onBackendKeyData(message: PgMessage.BackendKeyData) {
        logger.connectionLogger(this, Level.TRACE) {
            this.message = "Got backend key data. Process ID: {processId}, Secret Key: ****"
            payload = mapOf("payloadId" to message.processId)
        }
        backendKeyData = message
    }

    private fun onParameterStatus(message: PgMessage.ParameterStatus) {
        logger.connectionLogger(this, Level.TRACE) {
            this.message = "Parameter Status, {status}"
            payload = mapOf("status" to message)
        }
    }

    private suspend fun onReadyForQuery(message: PgMessage.ReadyForQuery) {
        logger.connectionLogger(this, Level.TRACE) {
            this.message = "Connection ready for query. Transaction Status: {status}"
            payload = mapOf("status" to message.transactionStatus)
        }
        resetQueryRunning()
        if (initialReadyForQuery.complete(Unit)) {
            logger.connectionLogger(this, Level.TRACE) {
                this.message = "Connection is now initialized and ready for queries"
            }
            return
        }
        queryDoneChannel.send(Unit)
    }

    private suspend fun onRowDescription(message: PgMessage.RowDescription) {
        logger.connectionLogger(this, Level.TRACE) {
            this.message = "Received row description -> {desc}"
            payload = mapOf("dec" to message)
        }
        rowDescriptionChannel.send(message.fields)
        currentPreparedStatement.value?.metadata = message.fields
    }

    private suspend fun onDataRow(message: PgMessage.DataRow) {
        logger.connectionLogger(this, Level.TRACE) {
            this.message = "Received row size = {size}"
            payload = mapOf("size" to message.values.size)
        }
        dataRowChannel.send(message)
    }

    private suspend fun onCommandComplete(message: PgMessage.CommandComplete) {
        logger.connectionLogger(this, Level.TRACE) {
            this.message = "Received command complete -> {message}"
            payload = mapOf("message" to message)
        }
        commandCompleteChannel.send(message)
    }

    private suspend fun onCloseComplete() {
        logger.connectionLogger(this, Level.TRACE) {
            this.message = "Received close complete"
        }
        if (releasePreparedStatement.getAndSet(false)) {
            closeStatementChannel.send(Unit)
        }
    }

    private suspend fun onCopyInResponse(message: PgMessage.CopyInResponse) {
        logger.connectionLogger(this, Level.TRACE) {
            this.message = "Received CopyInResponse -> {message}"
            payload = mapOf("message" to message)
        }
        copyInResponseChannel.send(Unit)
    }

    private suspend fun onCopyOutResponse(message: PgMessage.CopyOutResponse) {
        logger.connectionLogger(this, Level.TRACE) {
            this.message = "Received CopyOutResponse -> {message}"
            payload = mapOf("message" to message)
        }
        copyOutResponseChannel.send(Unit)
    }

    private suspend fun onCopyData(message: PgMessage.CopyData) {
        logger.connectionLogger(this, Level.TRACE) {
            this.message = "Received CopyData, size = {size}"
            payload = mapOf("size" to message.data.size)
        }
        copyDataChannel.send(message)
    }

    private suspend fun onCopyDone() {
        logger.connectionLogger(this, Level.TRACE) {
            this.message = "Received CopyDone"
        }
        copyDoneChannel.send(Unit)
    }

    internal suspend fun writeToStream(message: PgMessage) {
        val encoder = encoders.encoderFor(message)
        stream.writeMessage {
            encoder.encode(message, it)
        }
    }

    private suspend fun writeManyToStream(messages: Iterable<PgMessage>) {
        stream.writeMessage {
            for (message in messages) {
                val encoder = encoders.encoderFor(message)
                encoder.encode(message, it)
            }
        }
    }

    private suspend fun writeManyToStream(vararg messages: PgMessage) {
        stream.writeMessage {
            for (message in messages) {
                val encoder = encoders.encoderFor(message)
                encoder.encode(message, it)
            }
        }
    }

    private fun checkConnected() {
        check(isConnected) { "Cannot execute queries against a closed connection" }
    }

    private fun checkNotRunningQuery() {
        if (queryRunning.value) {
            logger.connectionLogger(this, Level.TRACE) {
                this.message = "Check to ensure query not running failed"
            }
            throw ConnectionRunningQuery(connectionId)
        }
    }

    private fun setReleasingStatement() {
        if (!releasePreparedStatement.compareAndSet(expect = false, update = true)) {
            error("Cannot release a prepared statement while releasing another")
        }
    }

    private fun resetQueryRunning() {
        if (!queryRunning.compareAndSet(expect = true, update = false)) {
            throw ConnectionNotRunningQuery(connectionId)
        }
    }

    private fun setQueryRunning() {
        if (!queryRunning.compareAndSet(expect = false, update = true)) {
            logger.connectionLogger(this, Level.TRACE) {
                this.message = "Query running already set to true"
            }
            throw ConnectionRunningQuery(connectionId)
        }
    }

    private fun addDataRow(resultSet: MutableResultSet, dataRow: PgMessage.DataRow) {
        val fields: Array<Any?> = Array(dataRow.values.size) { i ->
            dataRow.values[i]?.let {
                val columnType = resultSet.columnMapping[i]
                typeRegistry.decode(columnType, it, configuration.charset)
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
                    logger.connectionLogger(this, Level.ERROR) {
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
                logger.connectionLogger(this, Level.ERROR) {
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
                logger.connectionLogger(this, Level.ERROR) {
                    this.message = "Attempted to ROLLBACK a connection not within a transaction"
                }
            }
        }
    }

    override suspend fun sendQuery(query: String): QueryResult {
        require(query.isNotBlank()) { "Cannot send an empty query" }
        checkConnected()
        setQueryRunning()
        logger.connectionLogger(this, configuration.logSettings.statementLevel) {
            message = STATEMENT_TEMPLATE
            payload = mapOf("query" to query)
        }
        writeToStream(PgMessage.Query(query))

        var result = MutableResultSet(listOf())
        var commandComplete: PgMessage.CommandComplete? = null
        selectLoop {
            rowDescriptionChannel.onReceive {
                result = MutableResultSet(it)
                Loop.Continue
            }
            dataRowChannel.onReceive { dataRow ->
                addDataRow(result, dataRow)
                Loop.Continue
            }
            commandCompleteChannel.onReceive {
                commandComplete = it
                Loop.Continue
            }
            queryDoneChannel.onReceive {
                Loop.Break
            }
        }

        checkNotNull(commandComplete) {
            "Command complete message must exist before existing query. Something went really wrong"
        }

        return QueryResult(
            rowsAffected = commandComplete!!.rowCount,
            message = commandComplete!!.message,
            rows = result,
        )
    }

    override suspend fun sendPreparedStatement(
        query: String,
        parameters: List<Any?>,
        release: Boolean,
    ): QueryResult {
        require(query.isNotBlank()) { "Cannot send an empty query" }
        checkConnected()
        setQueryRunning()

        val statement = preparedStatements.getOrPut(query) {
            PgPreparedStatement(query)
        }

        require(statement.paramCount == parameters.size) {
            queryRunning.value = false
            """
            Query does not have the correct number of parameters.
            
            ${query.replaceIndent("            ")}
            
            Expected ${statement.paramCount}, got ${parameters.size}
            """.trimIndent()
        }

        currentPreparedStatement.value = statement
        var result = MutableResultSet(statement.metadata)

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
                portal =  statement.statementName,
                statementName =  statement.statementName,
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
            add(PgMessage.Sync)
        }
        logger.connectionLogger(this, configuration.logSettings.statementLevel) {
            message = STATEMENT_TEMPLATE
            payload = mapOf("query" to query)
        }
        writeManyToStream(messages)

        var commandComplete: PgMessage.CommandComplete? = null
        selectLoop {
            rowDescriptionChannel.onReceive {
                result = MutableResultSet(it)
                Loop.Continue
            }
            dataRowChannel.onReceive {
                addDataRow(result, it)
                Loop.Continue
            }
            commandCompleteChannel.onReceive {
                commandComplete = it
                Loop.Continue
            }
            queryDoneChannel.onReceive {
                Loop.Break
            }
        }

        if (release) {
            releasePreparedStatement(query)
        }
        checkNotNull(commandComplete) {
            "Command complete message must exist before existing query. Something went really wrong"
        }

        return QueryResult(commandComplete!!.rowCount, commandComplete!!.message, result)
    }

    override suspend fun releasePreparedStatement(query: String) {
        checkNotRunningQuery()
        setReleasingStatement()

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
        if (pool != null && pool!!.giveBack(this)) {
            return
        }
        try {
            if (stream.isActive) {
                writeToStream(PgMessage.Terminate)
            }
        } catch (ex: Throwable) {
            logger.connectionLogger(this, Level.WARN) {
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

    private fun validateCopyQuery(copyStatement: String) {
        require(COPY_CHECK_REGEX.containsMatchIn(copyStatement)) {
            "Invalid copy statement. Statement: $copyStatement"
        }
    }

    override suspend fun copyIn(copyInStatement: String, data: Flow<ByteArray>): QueryResult {
        checkConnected()
        setQueryRunning()
        validateCopyQuery(copyInStatement)

        logger.connectionLogger(this, configuration.logSettings.statementLevel) {
            message = STATEMENT_TEMPLATE
            payload = mapOf("query" to copyInStatement)
        }
        writeToStream(PgMessage.Query(copyInStatement))

        copyInResponseChannel.receive()
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

        val commandComplete = commandCompleteChannel.receive()
        queryDoneChannel.receive()

        return QueryResult(commandComplete.rowCount, commandComplete.message)
    }

    override suspend fun copyOut(copyOutStatement: String): ReceiveChannel<ByteArray> {
        checkConnected()
        setQueryRunning()
        validateCopyQuery(copyOutStatement)

        logger.connectionLogger(this, configuration.logSettings.statementLevel) {
            message = STATEMENT_TEMPLATE
            payload = mapOf("query" to copyOutStatement)
        }
        writeToStream(PgMessage.Query(copyOutStatement))

        copyOutResponseChannel.receive()

        val resultChannel = Channel<ByteArray>(capacity = Channel.BUFFERED)

        scope.launch {
            selectLoop {
                copyDataChannel.onReceive {
                    resultChannel.send(it.data)
                    Loop.Continue
                }
                copyDoneChannel.onReceive {
                    Loop.Continue
                }
                commandCompleteChannel.onReceive {
                    Loop.Continue
                }
                queryDoneChannel.onReceive {
                    resultChannel.close()
                    Loop.Break
                }
            }
        }

        return resultChannel
    }

    companion object {
        private val COPY_CHECK_REGEX = Regex("^copy\\s+", RegexOption.IGNORE_CASE)
        private const val STATEMENT_TEMPLATE = "Sending {query}"

        suspend fun connect(
            configuration: PgConnectOptions,
            charset: Charset,
            stream: PgStream,
            scope: CoroutineScope
        ): PgConnectionImpl {
            val connection = PgConnectionImpl(configuration, charset, stream, scope)
            connection.initialReadyForQuery.await()
            if (!connection.isConnected) {
                error("Could not initialize connection")
            }
            connection.typeRegistry.finalizeTypes(connection)
            return connection
        }
    }
}
