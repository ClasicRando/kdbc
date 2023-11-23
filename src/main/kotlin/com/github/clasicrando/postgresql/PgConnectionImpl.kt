package com.github.clasicrando.postgresql

import com.github.clasicrando.common.Loop
import com.github.clasicrando.common.SslMode
import com.github.clasicrando.common.atomic.AtomicMutableMap
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
import com.github.clasicrando.postgresql.column.PgTypeRegistry
import com.github.clasicrando.postgresql.message.CloseTarget
import com.github.clasicrando.postgresql.message.DescribeTarget
import com.github.clasicrando.postgresql.message.PgMessage
import com.github.clasicrando.postgresql.message.decoders.MessageDecoders
import com.github.clasicrando.postgresql.message.encoders.MessageEncoders
import com.github.clasicrando.postgresql.row.PgRowFieldDescription
import com.github.clasicrando.postgresql.stream.PgStream
import io.klogging.Klogging
import io.ktor.utils.io.charsets.Charset
import kotlinx.atomicfu.AtomicBoolean
import kotlinx.atomicfu.AtomicRef
import kotlinx.atomicfu.atomic
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.isActive
import kotlinx.coroutines.job
import kotlinx.coroutines.launch
import java.util.*

class PgConnectionImpl private constructor(
    internal val configuration: PgConnectOptions,
    private val charset: Charset,
    private val stream: PgStream,
    scope: CoroutineScope,
    private val typeRegistry: PgTypeRegistry = PgTypeRegistry(),
    private val encoders: MessageEncoders = MessageEncoders(charset, typeRegistry),
    private val decoders: MessageDecoders = MessageDecoders(charset),
    override val connectionId: String = UUID.randomUUID().toString(),
    override var pool: ConnectionPool? = null,
) : PgConnection, PoolConnection, Klogging {

    private var isAuthenticated = false
    private var backendKeyData: PgMessage.BackendKeyData? = null
    private val initialReadyForQuery: CompletableDeferred<Unit> = CompletableDeferred(
        parent = scope.coroutineContext.job,
    )
    private val _inTransaction: AtomicBoolean = atomic(false)
    override val inTransaction: Boolean get() = _inTransaction.value

    private val queryRunning: AtomicBoolean = atomic(true)
    private val rowDescriptionChannel = Channel<List<PgRowFieldDescription>>()
    private val dataRowChannel = Channel<PgMessage.DataRow>(capacity = Channel.BUFFERED)
    private val commandCompleteChannel = Channel<PgMessage.CommandComplete>()
    private val releasePreparedStatement: AtomicBoolean = atomic(false)
    private val closeStatementChannel = Channel<Unit>()
    private val queryDoneChannel = Channel<Unit>()

    private val preparedStatements: MutableMap<String, PgPreparedStatement> = AtomicMutableMap()
    private val currentPreparedStatement: AtomicRef<PgPreparedStatement?> = atomic(null)

    private val messageProcessorJob = scope.launch {
        sendStartupMessage()
        try {
            handleAuthFlow()
            while (isActive && stream.isActive) {
                processServerMessage()
            }
        } finally {
            logger.info("Server Message Processor is closing")
        }
    }

    internal suspend fun receiveServerMessage(): PgMessage {
        val rawMessage = stream.receiveMessage()
        return decoders.decode(rawMessage)
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
            else -> {
                logger.info("Received message: $message")
            }
        }
    }

    private suspend fun sendStartupMessage() {
        val startupMessage = if (configuration.sslMode == SslMode.Disable) {
            PgMessage.StartupMessage(params = configuration.properties)
        } else {
            PgMessage.SslRequest
        }
        writeToStream(startupMessage)
    }

    private suspend fun handleAuthFlow() {
        val message = receiveServerMessage()
        if (message is PgMessage.ErrorResponse) {
            onErrorMessage(message)
            close()
            error("Expected auth message but got error response. Closing connection")
        }
        if (message !is PgMessage.Authentication) {
            error("Server sent non-auth message that was not an error. Closing connection")
        }
        when (val auth = message.authentication) {
            Authentication.Ok -> {
                logger.info("Successfully logged in to database")
                isAuthenticated = true
            }
            Authentication.CleartextPassword -> {
                val passwordMessage = createSimplePasswordMessage()
                writeToStream(passwordMessage)
            }
            is Authentication.Md5Password -> {
                val passwordMessage = createSimplePasswordMessage(auth.salt)
                writeToStream(passwordMessage)
            }
            is Authentication.Sasl -> {
                isAuthenticated = saslAuthFlow(this, auth)
            }
            else -> error("Auth request type cannot be handled. $auth")
        }
        if (!isAuthenticated) {
            error("Not authenticated")
        }
    }

    private suspend fun onNotice(message: PgMessage.NoticeResponse) {
        logger.info("Notice, message -> {noticeResponse}", message)
    }

    private suspend fun onErrorMessage(message: PgMessage.ErrorResponse) {
        logger.error("Error, message -> {errorResponse}", message)
        val throwable = GeneralPostgresError(message)
        rowDescriptionChannel.close(cause = throwable)
        dataRowChannel.close(cause = throwable)
        commandCompleteChannel.close(cause = throwable)
        queryDoneChannel.close(cause = throwable)
        closeStatementChannel.close(cause = throwable)
    }

    private suspend fun onBackendKeyData(message: PgMessage.BackendKeyData) {
        logger.info(
            "Got backend key data. Process ID: {processId}, Secret Key: ****",
            message.processId,
        )
        backendKeyData = message
    }

    private suspend fun onParameterStatus(message: PgMessage.ParameterStatus) {
        logger.info("Parameter Status, {status}", message)
    }

    private suspend fun onReadyForQuery(message: PgMessage.ReadyForQuery) {
        logger.info(
            "Connection ready for query. Transaction Status: {status}",
            message.transactionStatus,
        )
        resetQueryRunning()
        if (initialReadyForQuery.complete(Unit)) {
            logger.info("Connection is now initialized and ready for queries")
            return
        }
        queryDoneChannel.send(Unit)
    }

    private suspend fun onRowDescription(message: PgMessage.RowDescription) {
        logger.info("Received row description -> {desc}", message)
        rowDescriptionChannel.send(message.fields)
        currentPreparedStatement.value?.metadata = message.fields
    }

    private suspend fun onDataRow(message: PgMessage.DataRow) {
        logger.info("Received row size = {size}", message.values.size)
        dataRowChannel.send(message)
    }

    private suspend fun onCommandComplete(message: PgMessage.CommandComplete) {
        logger.info("Received command complete -> {message}", message)
        commandCompleteChannel.send(message)
    }

    private suspend fun onCloseComplete() {
        logger.info("Received close complete")
        if (releasePreparedStatement.getAndSet(false)) {
            closeStatementChannel.send(Unit)
        }
    }

    private fun createSimplePasswordMessage(
        salt: ByteArray? = null,
    ): PgMessage.PasswordMessage {
        val passwordBytes = configuration.password
            ?.toByteArray(charset = charset)
            ?: byteArrayOf()
        val bytes = if (salt == null) {
            passwordBytes
        } else {
            PasswordHelper.encode(
                username = configuration.username.toByteArray(charset = charset),
                password = passwordBytes,
                salt = salt,
            )
        }
        return PgMessage.PasswordMessage(bytes)
    }

    internal suspend fun writeToStream(message: PgMessage) {
        logger.info("Writing message, -> {message}", message)
        val encoder = encoders.encoderFor(message)
        stream.writeMessage {
            encoder.encode(message, it)
        }
    }

    private suspend fun writeManyToStream(messages: Iterable<PgMessage>) {
        for (message in messages) {
            logger.info("Writing message, -> {message}", message)
            val encoder = encoders.encoderFor(message)
            stream.writeMessage {
                encoder.encode(message, it)
            }
        }
    }

    private suspend fun writeManyToStream(vararg messages: PgMessage) {
        for (message in messages) {
            logger.info("Writing message, -> {message}", message)
            val encoder = encoders.encoderFor(message)
            stream.writeMessage {
                encoder.encode(message, it)
            }
        }
    }

    private fun checkConnected() {
        check(isConnected) { "Cannot execute queries against a closed connection" }
    }

    private fun checkRunningQuery() {
        if (queryRunning.value) {
            throw ConnectionRunningQuery(connectionId)
        }
    }

    private fun checkNotRunningQuery() {
        if (!queryRunning.value) {
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

    override val isConnected: Boolean get() = stream.isActive

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
                    logger.error(
                        ex2,
                        "Error while trying to rollback. BEGIN called while in transaction"
                    )
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
                logger.error("Attempted to COMMIT a connection not within a transaction")
            }
        }
    }

    override suspend fun rollback() {
        try {
            sendQuery("ROLLBACK;")
        } finally {
            if (!_inTransaction.getAndSet(false)) {
                logger.error("Attempted to ROLLBACK a connection not within a transaction")
            }
        }
    }

    override suspend fun sendQuery(query: String): QueryResult {
        require(query.isNotBlank()) { "Cannot send an empty query" }
        checkConnected()
        setQueryRunning()
        writeToStream(PgMessage.Query(query))

        val rowDescription = rowDescriptionChannel.receive()
        val result = MutableResultSet(rowDescription)

        var commandComplete: PgMessage.CommandComplete? = null
        selectLoop {
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

        checkNotNull(commandComplete) {
            "Command complete message must exist before existing query. Something went really wrong"
        }

        return QueryResult(commandComplete!!.rowCount, commandComplete!!.message, result)
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

        val messages = buildList {
            if (!statement.prepared) {
                val parseMessage = PgMessage.Parse(
                    preparedStatementName = statement.statementId,
                    query = query,
                    parameters = parameters,
                )
                add(parseMessage)
            }
            val bindMessage = PgMessage.Bind(
                portal =  statement.statementId,
                statementName =  statement.statementId,
                parameters = parameters,
            )
            add(bindMessage)
            if (statement.metadata.isEmpty()) {
                val describeMessage = PgMessage.Describe(
                    target = DescribeTarget.Portal,
                    name = statement.statementId,
                )
                add(describeMessage)
            }
            val executeMessage = PgMessage.Execute(
                portalName = statement.statementId,
                maxRowCount = 0,
            )
            add(executeMessage)
            val closePortalMessage = PgMessage.Close(
                target = CloseTarget.Portal,
                targetName = statement.statementId,
            )
            add(closePortalMessage)
            add(PgMessage.Sync)
        }
        writeManyToStream(messages)

        val rowDescription = rowDescriptionChannel.receive()
        val result = MutableResultSet(rowDescription)

        var commandComplete: PgMessage.CommandComplete? = null
        selectLoop {
            dataRowChannel.onReceive {
                addDataRow(result, it)
                Loop.Continue
            }
            commandCompleteChannel.onReceive {
                commandComplete = it
                Loop.Continue
            }
            queryDoneChannel.onReceive {
                logger.info("Query Done")
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
            ?: error("Could not find prepared statement for query\n\n$query")
        val closeMessage = PgMessage.Close(
            target = CloseTarget.PreparedStatement,
            targetName = statement.statementId,
        )
        writeManyToStream(closeMessage, PgMessage.Sync)
        closeStatementChannel.receive()
        preparedStatements.remove(query)
    }

    override suspend fun close() {
        if (pool != null && pool!!.giveBack(this)) {
            return
        }
        writeToStream(PgMessage.Terminate)
        stream.close()
        if (messageProcessorJob.isActive) {
            messageProcessorJob.cancelAndJoin()
        }
    }

    override suspend fun copyIn(copyInStatement: String, data: Flow<ByteArray>): QueryResult {
        TODO("Not yet implemented")
    }

    override suspend fun copyOut(copyOutStatement: String): ReceiveChannel<ByteArray> {
        TODO("Not yet implemented")
    }

    companion object {
        suspend fun connect(
            configuration: PgConnectOptions,
            charset: Charset,
            stream: PgStream,
            scope: CoroutineScope
        ): PgConnectionImpl {
            val connection = PgConnectionImpl(configuration, charset, stream, scope)
            connection.initialReadyForQuery.await()
            return connection
        }
    }
}
