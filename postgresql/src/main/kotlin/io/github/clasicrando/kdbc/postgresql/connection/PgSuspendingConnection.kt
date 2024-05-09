package io.github.clasicrando.kdbc.postgresql.connection

import io.github.clasicrando.kdbc.core.DefaultUniqueResourceId
import io.github.clasicrando.kdbc.core.Loop
import io.github.clasicrando.kdbc.core.connection.SuspendingConnection
import io.github.clasicrando.kdbc.core.exceptions.UnexpectedTransactionState
import io.github.clasicrando.kdbc.core.query.QueryParameter
import io.github.clasicrando.kdbc.core.query.SuspendingPreparedQuery
import io.github.clasicrando.kdbc.core.query.SuspendingPreparedQueryBatch
import io.github.clasicrando.kdbc.core.query.SuspendingQuery
import io.github.clasicrando.kdbc.core.quoteIdentifier
import io.github.clasicrando.kdbc.core.reduceToSingleOrNull
import io.github.clasicrando.kdbc.core.resourceLogger
import io.github.clasicrando.kdbc.core.result.AbstractMutableResultSet
import io.github.clasicrando.kdbc.core.result.QueryResult
import io.github.clasicrando.kdbc.core.result.StatementResult
import io.github.clasicrando.kdbc.postgresql.GeneralPostgresError
import io.github.clasicrando.kdbc.postgresql.Postgres
import io.github.clasicrando.kdbc.postgresql.column.PgTypeCache
import io.github.clasicrando.kdbc.postgresql.copy.CopyStatement
import io.github.clasicrando.kdbc.postgresql.copy.CopyType
import io.github.clasicrando.kdbc.postgresql.message.MessageTarget
import io.github.clasicrando.kdbc.postgresql.message.PgMessage
import io.github.clasicrando.kdbc.postgresql.message.TransactionStatus
import io.github.clasicrando.kdbc.postgresql.notification.PgNotification
import io.github.clasicrando.kdbc.postgresql.pool.PgSuspendingConnectionPool
import io.github.clasicrando.kdbc.postgresql.query.PgSuspendingPreparedQuery
import io.github.clasicrando.kdbc.postgresql.query.PgSuspendingPreparedQueryBatch
import io.github.clasicrando.kdbc.postgresql.query.PgSuspendingQuery
import io.github.clasicrando.kdbc.postgresql.result.PgResultSet
import io.github.clasicrando.kdbc.postgresql.statement.PgEncodeBuffer
import io.github.clasicrando.kdbc.postgresql.statement.PgPreparedStatement
import io.github.clasicrando.kdbc.postgresql.stream.PgStream
import io.github.oshai.kotlinlogging.KLoggingEventBuilder
import io.github.oshai.kotlinlogging.KotlinLogging
import io.github.oshai.kotlinlogging.Level
import kotlinx.atomicfu.AtomicBoolean
import kotlinx.atomicfu.atomic
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.datetime.Clock

private val logger = KotlinLogging.logger {}

/**
 * [SuspendingConnection] object for a Postgresql database. A new instance cannot be created but
 * rather the [Postgres.suspendingConnection] method should be called to receive a new
 * [PgSuspendingConnection] ready for user usage. This method will use connection pooling behind
 * the scenes as to reduce unnecessary tcp connection creation to the server when an application
 * creates and closes connections frequently.
 */
class PgSuspendingConnection internal constructor(
    /** Connection options supplied when requesting a new Postgresql connection */
    private val connectOptions: PgConnectOptions,
    /** Underlining stream of data to and from the database */
    private val stream: PgStream,
    /** Reference to the connection pool that owns this connection */
    private val pool: PgSuspendingConnectionPool,
    /** Type registry for connection. Used to decode data rows returned by the server. */
    @PublishedApi internal val typeCache: PgTypeCache = pool.typeCache,
) : SuspendingConnection, DefaultUniqueResourceId() {
    private val _inTransaction: AtomicBoolean = atomic(false)
    override val inTransaction: Boolean get() = _inTransaction.value

    /**
     *
     */
    private val mutex = Mutex()
    /**
     * [ReceiveChannel] for [PgNotification]s received from the server. Although this method is
     * available to anyone who has a reference to this [PgSuspendingConnection], you should only
     * have one coroutine that consumes this channel since messages are not duplicated for multiple
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
     * the connection to interact with the server.
     */
    private fun checkConnected() {
        check(isConnected) { "Cannot execute queries against a closed connection" }
    }

    override val isConnected: Boolean get() = stream.isConnected

    override suspend fun begin() {
        try {
            sendSimpleQuery("BEGIN;")
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
            sendSimpleQuery("COMMIT;")
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
            sendSimpleQuery("ROLLBACK;")
        } finally {
            if (!_inTransaction.getAndSet(false)) {
                log(Level.ERROR) {
                    this.message = "Attempted to ROLLBACK a connection not within a transaction"
                }
            }
        }
    }

    override fun createQuery(query: String): SuspendingQuery = PgSuspendingQuery(this, query)

    override fun createPreparedQuery(query: String): SuspendingPreparedQuery {
        return PgSuspendingPreparedQuery(this, query)
    }

    override fun createPreparedQueryBatch(): SuspendingPreparedQueryBatch {
        return PgSuspendingPreparedQueryBatch(this)
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
    private suspend fun collectResults(
        isAutoCommit: Boolean,
        statements: Array<PgPreparedStatement> = emptyArray(),
    ): StatementResult {
        val builder = StatementResult.Builder()
        val errors = mutableListOf<Throwable>()
        for (preparedStatement in statements) {
            val result = PgResultSet(typeCache, preparedStatement.resultMetadata)
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
                        val queryResult = QueryResult(message.rowCount, message.message, result)
                        builder.addQueryResult(queryResult)
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
            if (errors.isNotEmpty() && !isAutoCommit) {
                break
            }
        }

        val error = errors.reduceToSingleOrNull() ?: return builder.build()
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
        var result = PgResultSet(typeCache, statement?.resultMetadata ?: listOf())
        val results = StatementResult.Builder()
        stream.processMessageLoop { message ->
            when (message) {
                is PgMessage.ErrorResponse -> {
                    errors.add(GeneralPostgresError(message))
                    Loop.Continue
                }
                is PgMessage.RowDescription -> {
                    statement?.resultMetadata = message.fields
                    result = PgResultSet(typeCache, message.fields)
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

    /**
     * Send a query to the postgres database using the simple query protocol. This sends a single
     * [PgMessage.Query] message with the raw SQL query with no parameters. The database then
     * responds with zero or more [QueryResult]s that are packages into a [StatementResult].
     *
     * **Note**: This method will defer to [sendExtendedQuery] if the [query] does not contain a
     * semicolon and the [PgConnectOptions.useExtendedProtocolForSimpleQueries] is true (the
     * default value).
     *
     * [postgres docs](https://www.postgresql.org/docs/current/protocol-flow.html#PROTOCOL-FLOW-SIMPLE-QUERY)
     *
     * @throws IllegalArgumentException if the [query] is blank
     * @throws IllegalStateException if the underlining connection is no longer active
     */
    internal suspend fun sendSimpleQuery(
        query: String,
    ): StatementResult {
        require(query.isNotBlank()) { "Cannot send an empty query" }
        checkConnected()

        if (!query.contains(";") && connectOptions.useExtendedProtocolForSimpleQueries) {
            return sendExtendedQuery(query, listOf())
        }

        return mutex.withLock {
            logger.resourceLogger(
                this@PgSuspendingConnection,
                connectOptions.logSettings.statementLevel,
            ) {
                message = STATEMENT_TEMPLATE
                payload = mapOf("query" to query)
            }
            stream.writeToStream(PgMessage.Query(query))

            collectResult()
        }
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
        val neverExecuted = preparedStatements.values
            .find { it.lastExecuted == null }
        if (neverExecuted != null) {
            releasePreparedStatement(neverExecuted)
            return
        }
        val oldestQuery = preparedStatements.values
            .asSequence()
            .filter { it.lastExecuted != null }
            .maxBy { it.lastExecuted!! }
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
        parameters: List<QueryParameter>,
    ): PgPreparedStatement {
        val statement = getOrCachePreparedStatement(query)

        require(statement.paramCount == parameters.size) {
            """
            Query does not have the correct number of parameters. Expected ${statement.paramCount}, got ${parameters.size}
            
            ${query.trim().replaceIndent("            ")}
            """.trimIndent()
        }

        if (!statement.prepared) {
            executeStatementPrepare(
                query = query,
                parameterTypes = parameters.map { typeCache.getTypeHint(it).oid },
                statement = statement,
            )
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
        parameters: PgEncodeBuffer,
        sendSync: Boolean = true,
    ) {
        stream.writeManyToStream {
            val bindMessage = PgMessage.Bind(
                portal = null,
                statementName = statement.statementName,
                encodeBuffer = parameters,
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
        statement.lastExecuted = Clock.System.now()
        log(connectOptions.logSettings.statementLevel) {
            message = STATEMENT_TEMPLATE
            payload = mapOf("query" to statement.query)
        }
    }

    /**
     * Send a query to the postgres database using the extended query protocol. This goes through
     * the process of preparing the query (see [prepareStatement]) before then executing with the
     * provided parameters. The database then responds with 1 [QueryResult]s that is packages into
     * a [StatementResult].
     *
     * [postgres docs](https://www.postgresql.org/docs/current/protocol-flow.html#PROTOCOL-FLOW-EXT-QUERY)
     *
     * @throws IllegalArgumentException if the [query] is blank
     * @throws IllegalStateException if the underlining connection is no longer active
     */
    internal suspend fun sendExtendedQuery(
        query: String,
        parameters: List<QueryParameter>,
    ): StatementResult  {
        require(query.isNotBlank()) { "Cannot send an empty query" }
        checkConnected()

        return mutex.withLock {
            val statement = try {
                prepareStatement(query, parameters)
            } catch (ex: Throwable) {
                throw ex
            }

            val encodeBuffer = PgEncodeBuffer(statement.resultMetadata, typeCache)
            for ((parameter, type) in parameters) {
                encodeBuffer.encodeValue(parameter, type)
            }
            executePreparedStatement(statement, encodeBuffer)
            collectResult(statement = statement)
        }
    }

    /**
     * Send a [PgMessage.Close] message for the [preparedStatement]. This will close the server
     * side prepared statement and then remove the [preparedStatement] for the client cache.
     */
    private suspend fun releasePreparedStatement(preparedStatement: PgPreparedStatement) {
        mutex.withLock {
            val closeMessage = PgMessage.Close(
                target = MessageTarget.PreparedStatement,
                targetName = preparedStatement.statementName,
            )
            stream.writeManyToStream(closeMessage, PgMessage.Sync)
            stream.waitForOrError<PgMessage.CommandComplete>()
            preparedStatements.remove(preparedStatement.query)
        }
    }

    /**
     * Dispose of all internal connection resources while also sending a [PgMessage.Terminate] so
     * the database server is alerted to the closure.
     */
    internal suspend fun dispose() {
        try {
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
        if (!pool.giveBack(this@PgSuspendingConnection)) {
            dispose()
        }
    }

    /**
     * Allows for vararg specification of prepared statements using [pipelineQueries] where syncAll
     * is the default true. See the other method doc for more information.
     *
     * @see pipelineQueries
     */
    internal suspend fun pipelineQueriesSyncAll(
        vararg queries: Pair<String, List<QueryParameter>>,
    ): Iterable<QueryResult> {
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
     * [sendExtendedQuery] or package your queries into a stored procedure.
     */
    internal suspend fun pipelineQueries(
        syncAll: Boolean = true,
        vararg queries: Pair<String, List<QueryParameter>>,
    ): StatementResult {
        return mutex.withLock {
            val statements = Array(queries.size) { i ->
                val (queryText, queryParams) = queries[i]
                prepareStatement(query = queryText, parameters = queryParams)
            }
            for ((i, statement) in statements.withIndex()) {
                val encodeBuffer = PgEncodeBuffer(statement.resultMetadata, typeCache)
                for ((parameter, type) in queries[i].second) {
                    encodeBuffer.encodeValue(parameter, type)
                }

                executePreparedStatement(
                    statement = statement,
                    parameters = encodeBuffer,
                    sendSync = syncAll || i == queries.size - 1,
                )
            }
            collectResults(syncAll, statements)
        }
    }

    private suspend fun copyInInternal(copyQuery: String, data: Flow<ByteArray>): QueryResult {
        log(connectOptions.logSettings.statementLevel) {
            message = STATEMENT_TEMPLATE
            payload = mapOf("query" to copyQuery)
        }
        stream.writeToStream(PgMessage.Query(copyQuery))
        stream.waitForOrError<PgMessage.CopyInResponse>()

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

        val copyQuery = copyInStatement.toStatement(CopyType.From)
        return mutex.withLock { copyInInternal(copyQuery, data) }
    }

    private suspend fun copyOutInternal(copyQuery: String): Flow<ByteArray> {
        log(connectOptions.logSettings.statementLevel) {
            message = STATEMENT_TEMPLATE
            payload = mapOf("query" to copyQuery)
        }
        stream.writeToStream(PgMessage.Query(copyQuery))
        stream.waitForOrError<PgMessage.CopyOutResponse>()

        return flow {
            stream.processMessageLoop { message ->
                when (message) {
                    is PgMessage.ErrorResponse -> {
                        throw GeneralPostgresError(message)
                    }
                    is PgMessage.CopyData -> {
                        emit(message.data)
                        Loop.Continue
                    }
                    is PgMessage.CopyDone, is PgMessage.CommandComplete -> Loop.Continue
                    is PgMessage.ReadyForQuery -> {
                        handleTransactionStatus(message.transactionStatus)
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
            }.getOrThrow()
        }
    }

    /**
     * Execute a `COPY TO` command using the options supplied in the [copyOutStatement], reading
     * each `CopyData` server response message and passing the data through the returned [Flow].
     * The returned [Flow] is cold so if you want to avoid suspending the server message processor,
     * you should always try to process each item as soon as possible or collect the elements into
     * a [List].
     */
    suspend fun copyOut(
        copyOutStatement: CopyStatement,
    ): Flow<ByteArray> {
        checkConnected()

        val copyQuery = copyOutStatement.toStatement(CopyType.To)
        return mutex.withLock { copyOutInternal(copyQuery) }
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
        sendSimpleQuery(query)
    }

    /**
     * Execute a `NOTIFY` command for the specified [channelName] with the supplied [payload]. This
     * sends a notification to any connection connected to this connection's current database.
     */
    suspend fun notify(channelName: String, payload: String) {
        val escapedPayload = payload.replace("'", "''")
        sendSimpleQuery("NOTIFY ${channelName.quoteIdentifier()}, '${escapedPayload}';")
    }

    /**
     * Register an [Enum] class as a new type available for encoding and decoding.
     *
     * @param type name of the type in the database (optionally schema qualified if not in public
     * schema)
     */
    suspend inline fun <reified E : Enum<E>> registerEnumType(type: String) {
        typeCache.addEnumType(
            connection = this,
            name = type,
            cls = E::class,
            enumValues = enumValues<E>(),
        )
    }

    /**
     * Register [T] as a new type composite type available for encoding and decoding.
     *
     * @param type name of the type in the database (optionally schema qualified if not in public
     * schema)
     */
    suspend inline fun <reified T : Any> registerCompositeType(type: String) {
        typeCache.addCompositeType(
            connection = this,
            name = type,
            cls = T::class,
        )
    }

    companion object {
        private const val STATEMENT_TEMPLATE = "Sending {query}"

        /**
         * Create a new [PgSuspendingConnection] instance using the supplied [connectOptions],
         * [stream] and [pool] (the pool that owns this connection).
         */
        internal suspend fun connect(
            connectOptions: PgConnectOptions,
            stream: PgStream,
            pool: PgSuspendingConnectionPool,
        ): PgSuspendingConnection {
            var connection: PgSuspendingConnection? = null
            try {
                connection = PgSuspendingConnection(connectOptions, stream, pool)
                if (!connection.isConnected) {
                    error("Could not initialize connection")
                }
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
