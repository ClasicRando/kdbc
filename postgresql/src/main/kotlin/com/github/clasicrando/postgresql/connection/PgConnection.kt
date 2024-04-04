package com.github.clasicrando.postgresql.connection

import com.github.clasicrando.common.DefaultUniqueResourceId
import com.github.clasicrando.common.Loop
import com.github.clasicrando.common.connection.Connection
import com.github.clasicrando.common.exceptions.UnexpectedTransactionState
import com.github.clasicrando.common.pool.ConnectionPool
import com.github.clasicrando.common.query.QueryBatch
import com.github.clasicrando.common.quoteIdentifier
import com.github.clasicrando.common.reduceToSingleOrNull
import com.github.clasicrando.common.resourceLogger
import com.github.clasicrando.common.result.AbstractMutableResultSet
import com.github.clasicrando.common.result.QueryResult
import com.github.clasicrando.common.result.StatementResult
import com.github.clasicrando.postgresql.GeneralPostgresError
import com.github.clasicrando.postgresql.column.PgTypeRegistry
import com.github.clasicrando.postgresql.column.compositeTypeDecoder
import com.github.clasicrando.postgresql.column.compositeTypeEncoder
import com.github.clasicrando.postgresql.column.enumTypeDecoder
import com.github.clasicrando.postgresql.column.enumTypeEncoder
import com.github.clasicrando.postgresql.copy.CopyStatement
import com.github.clasicrando.postgresql.copy.CopyType
import com.github.clasicrando.postgresql.message.MessageTarget
import com.github.clasicrando.postgresql.message.PgMessage
import com.github.clasicrando.postgresql.message.TransactionStatus
import com.github.clasicrando.postgresql.notification.PgNotification
import com.github.clasicrando.postgresql.pool.PgConnectionPool
import com.github.clasicrando.postgresql.pool.PgPoolManager
import com.github.clasicrando.postgresql.query.PgQueryBatch
import com.github.clasicrando.postgresql.result.PgResultSet
import com.github.clasicrando.postgresql.statement.PgArguments
import com.github.clasicrando.postgresql.statement.PgPreparedStatement
import com.github.clasicrando.postgresql.stream.PgStream
import io.github.oshai.kotlinlogging.KLoggingEventBuilder
import io.github.oshai.kotlinlogging.KotlinLogging
import io.github.oshai.kotlinlogging.Level
import kotlinx.atomicfu.AtomicBoolean
import kotlinx.atomicfu.atomic
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.map
import kotlinx.datetime.Clock
import kotlinx.datetime.TimeZone
import kotlinx.datetime.toLocalDateTime
import kotlin.reflect.typeOf

private val logger = KotlinLogging.logger {}

/**
 * [Connection] object for a Postgresql database. A new instance cannot be created but rather the
 * [PgConnection.connect] method should be called to receive a new [PgConnection] ready for user
 * usage. This method will use connection pooling behind the scenes as to reduce unnecessary tcp
 * connection creation to the server when an application creates and closes connections frequently.
 */
class PgConnection internal constructor(
    /** Connection options supplied when requesting a new Postgresql connection */
    private val connectOptions: PgConnectOptions,
    /** Underlining stream of data to and from the database */
    private val stream: PgStream,
    /** Reference to the [ConnectionPool] that owns this [PgConnection] */
    private val pool: PgConnectionPool,
    /** Type registry for connection. Used to decode data rows returned by the server. */
    @PublishedApi internal val typeRegistry: PgTypeRegistry = pool.typeRegistry,
) : Connection, DefaultUniqueResourceId() {
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
     * [ReceiveChannel] for [PgNotification]s received from the server. Although this method is
     * available to anyone who has a reference to this [PgConnection], you should only have one
     * coroutine that consumes this channel since messages are not duplicated for multiple
     * receivers so messages are consumed fairly in a first come, first server basis. The
     * recommended practice would then be to spawn a coroutine to read this channel until it closes
     * or consume this channel as a flow and dispatch each [PgNotification] to the desired
     * consumers, copying the [PgNotification] if needed.
     */
    val notifications: ReceiveChannel<PgNotification> get() = stream.notifications
    /**
     * Cache of [PgPreparedStatement] where the key is the query that initiated the prepared
     * statement. This is not thread safe, therefore it should only be accessed after querying
     * running has been disabled to ensure a single thread/coroutine is accessing the contents.
     */
    private val preparedStatements: MutableMap<String, PgPreparedStatement> = mutableMapOf()
    /** ID of the next prepared statement executed. Incremented after each statement is created */
    private var nextStatementId = 1u

    /**
     * Created a log message at the specified [level], applying the [block] to the
     * [KLogger.at][io.github.oshai.kotlinlogging.KLogger.at] method.
     */
    private inline fun log(level: Level, crossinline block: KLoggingEventBuilder.() -> Unit) {
        logger.resourceLogger(this, level, block)
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

    override val isConnected: Boolean get() = stream.isConnected

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

    private fun logUnexpectedMessage(message: PgMessage): Loop {
        log(Level.TRACE) {
            this.message = "Ignoring {message} since it's not an error or the desired type"
            payload = mapOf("message" to message)
        }
        return Loop.Continue
    }

    private suspend fun handleTransactionStatus(transactionStatus: TransactionStatus) {
        if (transactionStatus == TransactionStatus.FailedTransaction
            && connectOptions.autoRollbackOnFailedTransaction)
        {
            log(Level.TRACE) {
                this.message = "Server reported failed transaction. Issuing rollback command"
            }
            try {
                rollback()
            } catch (ex: Throwable) {
                log(Level.WARN) {
                    this.message = "Failed to rollback transaction after failed transaction status"
                    cause = ex
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
        statements: Array<PgPreparedStatement> = emptyArray(),
    ): Flow<QueryResult> = flow {
        val errors = mutableListOf<Throwable>()
        for ((i, preparedStatement) in statements.withIndex()) {
            val result = PgResultSet(typeRegistry, preparedStatement.resultMetadata)
            stream.processMessageLoop { message ->
                when (message) {
                    is PgMessage.ErrorResponse -> {
                        errors.add(GeneralPostgresError(message))
                        Loop.Continue
                    }
                    is PgMessage.DataRow -> {
                        result.addRow(message.rowBuffer)
                        Loop.Continue
                    }
                    is PgMessage.CommandComplete -> {
                        emit(QueryResult(message.rowCount, message.message, result))
                        Loop.Continue
                    }
                    is PgMessage.ReadyForQuery -> {
                        handleTransactionStatus(message.transactionStatus)
                        if (i == statements.size - 1) {
                            enableQueryRunning()
                        }
                        Loop.Break
                    }
                    else -> logUnexpectedMessage(message)
                }
            }.onFailure {
                errors.add(it)
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
    }

    /**
     * Collect all [QueryResult]s for the query as a buffered [Flow].
     *
     * This allows for multiple results sets from a single query to be collected by continuously
     * looping over server messages through [PgStream.processMessageLoop], exiting only once a
     * query done message is received. There will only ever be multiple results if the
     * [statement] parameter is null (i.e. it's a simple query).
     *
     * Within the message loop, it receives messages that are:
     * - error -> packing into error collection
     * - row description -> updating the [statement], if any, and creating a new
     * [AbstractMutableResultSet]
     * - data row -> pack the row into the current [AbstractMutableResultSet]
     * - command complete -> emitting a new [QueryResult] from the [flow]
     * - query done -> enabling others to query against this connection and exiting the
     * selectLoop
     *
     * After the query collection, the collection of errors is checked and aggregated into one
     * [Throwable] (if any) and thrown. Otherwise, the [flow] exits with all [QueryResult]s
     * yielded.
     */
    private suspend fun collectResult(
        statement: PgPreparedStatement? = null,
    ): StatementResult {
        val errors = mutableListOf<Throwable>()
        var result = PgResultSet(typeRegistry, statement?.resultMetadata ?: listOf())
        val results = StatementResult.Builder()
        stream.processMessageLoop { message ->
            when (message) {
                is PgMessage.ErrorResponse -> {
                    errors.add(GeneralPostgresError(message))
                    Loop.Continue
                }
                is PgMessage.RowDescription -> {
                    statement?.resultMetadata = message.fields
                    result = PgResultSet(typeRegistry, message.fields)
                    Loop.Continue
                }
                is PgMessage.DataRow -> {
                    result.addRow(message.rowBuffer)
                    Loop.Continue
                }
                is PgMessage.CommandComplete -> {
                    val queryResult = QueryResult(message.rowCount, message.message, result)
                    results.addQueryResult(queryResult)
                    Loop.Continue
                }
                is PgMessage.ReadyForQuery -> {
                    handleTransactionStatus(message.transactionStatus)
                    enableQueryRunning()
                    Loop.Break
                }
                else -> logUnexpectedMessage(message)
            }
        }.onFailure {
            errors.add(it)
        }

        val error = errors.reduceToSingleOrNull() ?: return results.build()
        log(Level.ERROR) {
            message = "Error during single query execution"
            cause = error
        }
        throw error
    }

    override suspend fun sendQuery(
        query: String,
    ): StatementResult {
        require(query.isNotBlank()) { "Cannot send an empty query" }
        checkConnected()
        waitForQueryRunning()
        logger.resourceLogger(
            this@PgConnection,
            connectOptions.logSettings.statementLevel,
        ) {
            message = STATEMENT_TEMPLATE
            payload = mapOf("query" to query)
        }
        stream.writeToStream(PgMessage.Query(query))

        return collectResult()
    }

    /**
     * Prepare the specified [statement] by requesting the server parse and describe the prepared
     * statement. The messages received from the server are then handled to populate data within
     * the [statement].
     */
    private suspend fun executeStatementPrepare(
        query: String,
        parameterTypes: List<Int>,
        statement: PgPreparedStatement,
    ) {
        val parseMessage = PgMessage.Parse(
            preparedStatementName = statement.statementName,
            query = query,
            parameterTypes = parameterTypes,
        )
        val describeMessage = PgMessage.Describe(
            target = MessageTarget.PreparedStatement,
            name = statement.statementName,
        )
        stream.writeManyToStream(parseMessage, describeMessage, PgMessage.Sync)

        val errors = mutableListOf<Throwable>()
        stream.processMessageLoop { message ->
            when (message) {
                is PgMessage.ErrorResponse -> {
                    errors.add(GeneralPostgresError(message))
                    Loop.Continue
                }
                is PgMessage.ParseComplete -> {
                    statement.prepared = true
                    Loop.Continue
                }
                is PgMessage.RowDescription -> {
                    statement.resultMetadata = message.fields.map { it.copy(formatCode = 1) }
                    Loop.Continue
                }
                is PgMessage.ParameterDescription -> {
                    statement.parameterTypeOids = message.parameterDataTypes
                    Loop.Continue
                }
                is PgMessage.NoData -> {
                    statement.resultMetadata = emptyList()
                    Loop.Continue
                }
                is PgMessage.ReadyForQuery -> {
                    handleTransactionStatus(message.transactionStatus)
                    Loop.Break
                }
                else -> logUnexpectedMessage(message)
            }
        }.onFailure {
            errors.add(it)
        }

        val error = errors.reduceToSingleOrNull() ?: return
        log(Level.ERROR) {
            message = "Error during prepared statement creation"
            cause = error
        }
        throw error
    }

    /**
     * Remove the oldest [PgPreparedStatement] in [preparedStatements]. If any statement has a null
     * [PgPreparedStatement.lastExecuted] then that statement is preferentially removed since the
     * statement was never executed.
     */
    private suspend fun removeOldestPreparedStatement() {
        val neverExecuted = preparedStatements.entries
            .find { it.value.lastExecuted == null }
            ?.key
        if (neverExecuted != null) {
            releasePreparedStatement(neverExecuted)
            return
        }
        val oldestQuery = preparedStatements.entries
            .asSequence()
            .filter { it.value.lastExecuted != null }
            .maxBy { it.value.lastExecuted!! }
            .key
        releasePreparedStatement(oldestQuery)
    }

    /**
     * Check the prepared statement cache to ensure the capacity is not exceeded. Continuously
     * removes statements until the cache is the right size. Once that condition is met, a new
     * entry is added to the cache for the current [query].
     */
    private suspend fun getOrCachePreparedStatement(query: String): PgPreparedStatement {
        while (preparedStatements.size >= connectOptions.statementCacheCapacity.toInt()) {
            removeOldestPreparedStatement()
        }
        return preparedStatements.getOrPut(query) {
            PgPreparedStatement(query, nextStatementId++)
        }
    }

    /**
     * Fetch a [PgPreparedStatement] from the cache for the provided [query], returning the
     * statement. If a statement does not already exist for the [query] then a new statement is
     * created in a non-prepared state. If the statement has not already been prepared then
     * [executePreparedStatement] is called to populate the [PgPreparedStatement] with the required
     * data.
     *
     * @throws IllegalArgumentException if the number of [parameters] does not match the number of
     * parameters required by the query
     */
    private suspend fun prepareStatement(
        query: String,
        parameters: List<Any?>,
    ): PgPreparedStatement {
        val statement = getOrCachePreparedStatement(query)

        require(statement.paramCount == parameters.size) {
            """
            Query does not have the correct number of parameters. Expected ${statement.paramCount}, got ${parameters.size}
            
            ${query.trim().replaceIndent("            ")}
            """.trimIndent()
        }

        if (!statement.prepared) {
            val parameterTypes = parameters.map {
                typeRegistry.kindOf(it).oid
            }
            executeStatementPrepare(query, parameterTypes, statement)
        }

        return statement
    }

    /**
     * Send all required messages to the server for prepared [statement] execution.
     *
     * - [PgMessage.Bind] with statement name + current [parameters]
     * - If the statement has not received metadata, [PgMessage.Describe] is sent
     * - [PgMessage.Execute] with the statement name
     * - [PgMessage.Close] with the statement name
     * - If [sendSync] is true, [PgMessage.Sync] is sent
     */
    private suspend fun executePreparedStatement(
        statement: PgPreparedStatement,
        parameters: List<Any?>,
        sendSync: Boolean = true,
    ) {
        stream.writeManyToStream {
            val bindMessage = PgMessage.Bind(
                portal = null,
                statementName = statement.statementName,
                parameters = PgArguments(typeRegistry, parameters, statement),
            )
            yield(bindMessage)
            val executeMessage = PgMessage.Execute(
                portalName = null,
                maxRowCount = 0,
            )
            yield(executeMessage)
            val closePortalMessage = PgMessage.Close(MessageTarget.Portal, null)
            yield(closePortalMessage)
            if (sendSync) {
                yield(PgMessage.Sync)
            }
        }
        statement.lastExecuted = Clock.System.now().toLocalDateTime(TimeZone.UTC)
        log(connectOptions.logSettings.statementLevel) {
            message = STATEMENT_TEMPLATE
            payload = mapOf("query" to statement.query)
        }
    }

    override suspend fun sendPreparedStatement(
        query: String,
        parameters: List<Any?>,
    ): StatementResult  {
        require(query.isNotBlank()) { "Cannot send an empty query" }
        checkConnected()
        waitForQueryRunning()
        val statement = try {
            prepareStatement(query, parameters)
        } catch (ex: Throwable) {
            enableQueryRunning()
            throw ex
        }
        executePreparedStatement(statement, parameters)
        return collectResult(statement = statement)
    }

    override suspend fun releasePreparedStatement(
        query: String,
    ) {
        waitForQueryRunning()

        val statement = preparedStatements[query]
        if (statement == null) {
            log(Level.WARN) {
                message = "query supplied did not match a stored prepared statement"
                payload = mapOf("query" to query)
            }
            enableQueryRunning()
            return
        }
        val closeMessage = PgMessage.Close(
            target = MessageTarget.PreparedStatement,
            targetName = statement.statementName,
        )
        stream.writeManyToStream(closeMessage, PgMessage.Sync)
        stream.waitForOrError<PgMessage.CommandComplete>()
            .onFailure {
                enableQueryRunning()
                throw it
            }
        preparedStatements.remove(query)
        enableQueryRunning()
    }

    /**
     * Dispose of all internal connection resources while also sending a [PgMessage.Terminate] so
     * the database server is alerted to the closure.
     */
    internal suspend fun dispose() {
        try {
            canRunQuery.close()
            if (stream.isConnected) {
                stream.writeToStream(PgMessage.Terminate)
                log(Level.TRACE) {
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
        preparedStatements.clear()
    }

    override suspend fun close() {
        if (!pool.giveBack(this@PgConnection)) {
            dispose()
        }
    }

    /**
     * Allows for vararg specification of prepared statements using [pipelineQueries] where syncAll
     * is the default true. See the other method doc for more information.
     *
     * @see pipelineQueries
     */
    suspend fun pipelineQueriesSyncAll(
        vararg queries: Pair<String, List<Any?>>,
    ): Flow<QueryResult> {
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
     * [sendPreparedStatement] or package your queries into a stored procedure.
     */
    suspend fun pipelineQueries(
        syncAll: Boolean = true,
        vararg queries: Pair<String, List<Any?>>,
    ): Flow<QueryResult> {
        waitForQueryRunning()
        val statements = try {
            Array(queries.size) { i ->
                val (queryText, queryParams) = queries[i]
                prepareStatement(query = queryText, parameters = queryParams)
            }
        } catch (ex: Throwable) {
            enableQueryRunning()
            throw ex
        }
        for ((i, statement) in statements.withIndex()) {
            executePreparedStatement(
                statement = statement,
                parameters = queries[i].second,
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
    suspend fun copyIn(
        copyInStatement: CopyStatement,
        data: Flow<ByteArray>,
    ): QueryResult {
        checkConnected()
        waitForQueryRunning()

        val copyQuery = copyInStatement.toStatement(CopyType.From)
        log(connectOptions.logSettings.statementLevel) {
            message = STATEMENT_TEMPLATE
            payload = mapOf("query" to copyQuery)
        }
        stream.writeToStream(PgMessage.Query(copyQuery))
        stream.waitForOrError<PgMessage.CopyInResponse>()
            .onFailure {
                enableQueryRunning()
                throw it
            }

        var wasFailed = false
        try {
            stream.writeManySized(data.map { PgMessage.CopyData(it) })
            stream.writeToStream(PgMessage.CopyDone)
        } catch (ex: Throwable) {
            if (stream.isConnected) {
                wasFailed = true
                stream.writeToStream(PgMessage.CopyFail("Exception collecting data\nError:\n$ex"))
            }
        }

        var completeMessage: PgMessage.CommandComplete? = null
        val errors = mutableListOf<Throwable>()
        stream.processMessageLoop { message ->
            when (message) {
                is PgMessage.ErrorResponse -> {
                    if (wasFailed) {
                        completeMessage = PgMessage.CommandComplete(
                            rowCount = 0,
                            message = "CopyFail Issued by client",
                        )
                        log(Level.WARN) {
                            this.message = "CopyIn operation failed by client"
                        }
                        return@processMessageLoop Loop.Continue
                    }
                    val error = GeneralPostgresError(message)
                    log(Level.ERROR) {
                        this.message = "Error during copy in operation"
                        cause = error
                    }
                    errors.add(error)
                    Loop.Continue
                }
                is PgMessage.CommandComplete -> {
                    completeMessage = message
                    Loop.Continue
                }
                is PgMessage.ReadyForQuery -> {
                    handleTransactionStatus(message.transactionStatus)
                    enableQueryRunning()
                    Loop.Break
                }
                else -> {
                    log(Level.TRACE) {
                        this.message = "Ignoring {message} since it's not an error or the desired type"
                        payload = mapOf("message" to message)
                    }
                    Loop.Continue
                }
            }
        }.onFailure {
            errors.add(it)
        }

        val error = errors.reduceToSingleOrNull()
        if (error != null) {
            throw error
        }

        return QueryResult(
            rowsAffected = completeMessage?.rowCount ?: 0,
            message= completeMessage?.message ?: "Default copy in complete message",
        )
    }

    /**
     * Execute a `COPY TO` command using the options supplied in the [copyOutStatement], reading
     * each `CopyData` server response message and passing the data through the returned buffered
     * [Flow]. The returned [Flow]'s buffer matches the [Channel.BUFFERED] behaviour so if you
     * want to avoid suspending the server message processor, you should always try to process each
     * item as soon as possible or collect the elements into a [List].
     */
    suspend fun copyOut(
        copyOutStatement: CopyStatement,
    ): Flow<ByteArray> {
        checkConnected()
        waitForQueryRunning()

        val copyQuery = copyOutStatement.toStatement(CopyType.To)
        log(connectOptions.logSettings.statementLevel) {
            message = STATEMENT_TEMPLATE
            payload = mapOf("query" to copyQuery)
        }
        stream.writeToStream(PgMessage.Query(copyQuery))
        stream.waitForOrError<PgMessage.CopyOutResponse>()
            .onFailure {
                enableQueryRunning()
                throw it
            }

        return flow {
            stream.processMessageLoop { message ->
                when (message) {
                    is PgMessage.ErrorResponse -> {
                        enableQueryRunning()
                        throw GeneralPostgresError(message)
                    }
                    is PgMessage.CopyData -> {
                        emit(message.data)
                        Loop.Continue
                    }
                    is PgMessage.CopyDone, is PgMessage.CommandComplete -> Loop.Continue
                    is PgMessage.ReadyForQuery -> {
                        handleTransactionStatus(message.transactionStatus)
                        enableQueryRunning()
                        Loop.Break
                    }
                    else -> {
                        log(Level.TRACE) {
                            this.message = "Ignoring {message} since it's not an error or the desired type"
                            payload = mapOf("message" to message)
                        }
                        Loop.Continue
                    }
                }
            }.onFailure {
                throw it
            }
        }
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

    /**
     * Update the type registry to allow for encoding and decoding PostGIS types. This should
     * happen at the beginning of your application or whenever you create an initial connection
     * using a unique [PgConnectOptions] instance since the type cache is shared between
     * connections with the same options. This also avoids inconsistency when adding and querying
     * in a concurrent environment.
     */
    fun includePostGisTypes() {
        typeRegistry.includePostGisTypes()
    }

    /**
     * Register an [Enum] class as a new type available for encoding and decoding.
     *
     * @param type name of the type in the database (optionally schema qualified if not in public
     * schema)
     */
    suspend inline fun <reified E : Enum<E>> registerEnumType(type: String) {
        typeRegistry.registerEnumType(
            connection = this,
            encoder = enumTypeEncoder<E>(type),
            decoder = enumTypeDecoder<E>(),
            type = type,
            arrayType = typeOf<List<E?>>(),
        )
    }

    /**
     * Register [T] as a new type composite type available for encoding and decoding.
     *
     * @param type name of the type in the database (optionally schema qualified if not in public
     * schema)
     */
    suspend inline fun <reified T : Any> registerCompositeType(type: String) {
        val encoder = compositeTypeEncoder<T>(type, typeRegistry)
        val decoder = compositeTypeDecoder<T>(typeRegistry)
        typeRegistry.registerCompositeType(
            connection = this,
            encoder = encoder,
            decoder = decoder,
            type = type,
            arrayType = typeOf<List<T?>>(),
        )
    }

    override fun createQueryBatch(): QueryBatch = PgQueryBatch(this)

    companion object {
        private const val STATEMENT_TEMPLATE = "Sending {query}"

        /**
         * Create a new [PgConnection] (or reuse an existing connection if any are available) using
         * the supplied [PgConnectOptions].
         */
        suspend fun connect(connectOptions: PgConnectOptions): PgConnection {
            return PgPoolManager.acquireConnection(connectOptions)
        }

        /**
         * Create a new [PgConnection] instance using the supplied [connectOptions], [stream] and
         * [pool] (the pool that owns this connection).
         */
        internal suspend fun connect(
            connectOptions: PgConnectOptions,
            stream: PgStream,
            pool: PgConnectionPool,
        ): PgConnection {
            var connection: PgConnection? = null
            try {
                connection = PgConnection(connectOptions, stream, pool)
                if (!connection.isConnected) {
                    error("Could not initialize connection")
                }
                connection.enableQueryRunning()
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