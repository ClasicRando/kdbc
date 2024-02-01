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
import com.github.clasicrando.postgresql.column.PgTypeRegistry
import com.github.clasicrando.postgresql.copy.CopyStatement
import com.github.clasicrando.postgresql.copy.CopyType
import com.github.clasicrando.postgresql.message.CloseTarget
import com.github.clasicrando.postgresql.message.DescribeTarget
import com.github.clasicrando.postgresql.message.PgMessage
import com.github.clasicrando.postgresql.notification.PgNotification
import com.github.clasicrando.postgresql.pool.PgPoolManager
import com.github.clasicrando.postgresql.statement.PgPreparedStatement
import com.github.clasicrando.postgresql.stream.PgStream
import io.github.oshai.kotlinlogging.KLoggingEventBuilder
import io.github.oshai.kotlinlogging.KotlinLogging
import io.github.oshai.kotlinlogging.Level
import kotlinx.atomicfu.AtomicBoolean
import kotlinx.atomicfu.atomic
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.FlowCollector
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.buffer
import kotlinx.coroutines.flow.flow
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
    private val connectOptions: PgConnectOptions,
    /** Underlining stream of data to and from the database */
    private val stream: PgStream,
    /** Reference to the [ConnectionPool] that owns this [PgConnection] */
    private val pool: ConnectionPool<PgConnection>,
    /**
     * Type registry for connection. Used to decode data rows returned by the server. Defaults to a
     * new instance of [PgTypeRegistry]
     */
    private val typeRegistry: PgTypeRegistry = PgTypeRegistry(),
    override val connectionId: UUID = UUID.generateUUID(),
) : Connection {
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
     * [ReceiveChannel] for [PgNotification]s received by the server. Although this method is
     * available to anyone who has a reference to this [PgConnection], you should only have one
     * coroutine that consumes this channel since messages are not duplicated for multiple
     * receivers so messages are consumed fairly in a first come, first server basis. The
     * recommended practice would then be to spawn a coroutine to read this channel until it closes
     * or consume this channel as a flow and dispatch each [PgNotification] to the desired
     * consumers, copying the [PgNotification] if needed.
     */
    val notifications: ReceiveChannel<PgNotification> get() = stream.notifications
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
    private inline fun log(level: Level, crossinline block: KLoggingEventBuilder.() -> Unit) {
        logger.connectionLogger(this, level, block)
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
                stream.errors.onReceive {
                    errors.add(it)
                    Loop.Continue
                }
                stream.rowDescription.onReceive {
                    preparedStatement?.metadata = it
                    result = MutableResultSet(it)
                    Loop.Continue
                }
                stream.dataRows.onReceive {
                    addDataRow(result, it)
                    Loop.Continue
                }
                stream.commandComplete.onReceive {
                    emit(QueryResult(it.rowCount, it.message, result))
                    Loop.Continue
                }
                stream.queryDone.onReceive {
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
    }

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
    private suspend fun collectResult(
        statement: PgPreparedStatement? = null,
    ): Iterable<QueryResult> {
        val errors = mutableListOf<Throwable>()
        var result = MutableResultSet(statement?.metadata ?: listOf())
        val results = mutableListOf<QueryResult>()
        selectLoop {
            stream.errors.onReceive {
                errors.add(it)
                Loop.Continue
            }
            stream.rowDescription.onReceive {
                statement?.metadata = it
                result = MutableResultSet(it)
                Loop.Continue
            }
            stream.dataRows.onReceive {
                addDataRow(result, it)
                Loop.Continue
            }
            stream.commandComplete.onReceive {
                results.add(QueryResult(it.rowCount, it.message, result))
                Loop.Continue
            }
            stream.queryDone.onReceive {
                enableQueryRunning()
                Loop.Break
            }
        }

        val error = errors.reduceToSingleOrNull() ?: return results
        log(Level.ERROR) {
            message = "Error during single query execution"
            cause = error
        }
        throw error
    }

    override suspend fun sendQuery(query: String): Iterable<QueryResult> {
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
        stream.writeToStream(PgMessage.Query(query))

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
                    parameterTypes = parameters.map { typeRegistry.kindOf(it) },
                )
                statement.prepared = true
                add(parseMessage)
            }
            val bindMessage = PgMessage.Bind(
                portal = statement.statementName,
                statementName = statement.statementName,
                parameters = parameters.map {
                    if (it == null) {
                        return@map null
                    }

                    val encodedValue = typeRegistry.encode(it)
                    encodedValue?.toByteArray(charset = connectOptions.charset)
                },
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
        stream.writeManyToStream(messages)
        return statement
    }

    override suspend fun sendPreparedStatement(
        query: String,
        parameters: List<Any?>,
    ): Iterable<QueryResult> {
        require(query.isNotBlank()) { "Cannot send an empty query" }
        checkConnected()
        waitForQueryRunning()
        val statement = sendPreparedStatementMessage(query, parameters)
        return collectResult(statement = statement)
    }

    override suspend fun releasePreparedStatement(query: String) {
        waitForQueryRunning()

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
        stream.writeManyToStream(closeMessage, PgMessage.Sync)
        waitOrError(stream.errors, stream.closeStatement)
        preparedStatements.remove(query)
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
     * [sendPreparedStatement] or package your queries into a stored procedure.
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
        stream.writeToStream(PgMessage.Query(copyQuery))

        waitOrError(stream.errors, stream.copyInResponse)

        val copyInJob = pool.launch {
            try {
                data.collect {
                    stream.writeToStream(PgMessage.CopyData(it))
                }
                stream.writeToStream(PgMessage.CopyDone)
            } catch (ex: Throwable) {
                if (stream.isConnected) {
                    stream.writeToStream(PgMessage.CopyFail("Exception collecting data\nError:\n$ex"))
                }
            }
        }

        var completeMessage: PgMessage.CommandComplete? = null
        val errors = mutableListOf<Throwable>()
        try {
            selectLoop {
                stream.errors.onReceive {
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
                stream.commandComplete.onReceive {
                    completeMessage = it
                    Loop.Continue
                }
                stream.queryDone.onReceive {
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
        stream.writeToStream(PgMessage.Query(copyQuery))

        waitOrError(stream.errors, stream.copyOutResponse)
        return flow {
            selectLoop {
                stream.errors.onReceive {
                    enableQueryRunning()
                    throw it
                }
                stream.copyData.onReceive {
                    emit(it.data)
                    Loop.Continue
                }
                stream.copyDone.onReceive {
                    Loop.Continue
                }
                stream.commandComplete.onReceive {
                    Loop.Continue
                }
                stream.queryDone.onReceive {
                    enableQueryRunning()
                    Loop.Break
                }
            }
        }
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
         * [pool] (the pool that owns this connection).
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
                if (!connection.isConnected) {
                    error("Could not initialize connection")
                }
                connection.typeRegistry.finalizeTypes(connection)
                connection.canRunQuery.send(Unit)
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