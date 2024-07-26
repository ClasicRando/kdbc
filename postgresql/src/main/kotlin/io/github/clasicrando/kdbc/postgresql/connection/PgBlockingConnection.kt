package io.github.clasicrando.kdbc.postgresql.connection

import com.github.doyaaaaaken.kotlincsv.dsl.csvWriter
import io.github.clasicrando.kdbc.core.DefaultUniqueResourceId
import io.github.clasicrando.kdbc.core.Loop
import io.github.clasicrando.kdbc.core.chunkedBytes
import io.github.clasicrando.kdbc.core.config.Kdbc
import io.github.clasicrando.kdbc.core.connection.BlockingConnection
import io.github.clasicrando.kdbc.core.exceptions.UnexpectedTransactionState
import io.github.clasicrando.kdbc.core.logWithResource
import io.github.clasicrando.kdbc.core.query.BlockingPreparedQuery
import io.github.clasicrando.kdbc.core.query.BlockingPreparedQueryBatch
import io.github.clasicrando.kdbc.core.query.BlockingQuery
import io.github.clasicrando.kdbc.core.query.QueryParameter
import io.github.clasicrando.kdbc.core.query.bind
import io.github.clasicrando.kdbc.core.query.fetchAll
import io.github.clasicrando.kdbc.core.quoteIdentifier
import io.github.clasicrando.kdbc.core.reduceToSingleOrNull
import io.github.clasicrando.kdbc.core.result.QueryResult
import io.github.clasicrando.kdbc.core.result.StatementResult
import io.github.clasicrando.kdbc.postgresql.GeneralPostgresError
import io.github.clasicrando.kdbc.postgresql.column.CompositeTypeDefinition
import io.github.clasicrando.kdbc.postgresql.column.PgTypeCache
import io.github.clasicrando.kdbc.postgresql.copy.CopyOutCollector
import io.github.clasicrando.kdbc.postgresql.copy.CopyStatement
import io.github.clasicrando.kdbc.postgresql.copy.CopyTableMetadata
import io.github.clasicrando.kdbc.postgresql.copy.PgBinaryCopyRow
import io.github.clasicrando.kdbc.postgresql.copy.PgCsvCopyRow
import io.github.clasicrando.kdbc.postgresql.copy.pgBinaryCopyHeader
import io.github.clasicrando.kdbc.postgresql.copy.pgBinaryCopyTrailer
import io.github.clasicrando.kdbc.postgresql.message.MessageTarget
import io.github.clasicrando.kdbc.postgresql.message.PgMessage
import io.github.clasicrando.kdbc.postgresql.message.TransactionStatus
import io.github.clasicrando.kdbc.postgresql.notification.PgNotification
import io.github.clasicrando.kdbc.postgresql.pool.PgBlockingConnectionPool
import io.github.clasicrando.kdbc.postgresql.query.PgBlockingPreparedQuery
import io.github.clasicrando.kdbc.postgresql.query.PgBlockingPreparedQueryBatch
import io.github.clasicrando.kdbc.postgresql.query.PgBlockingQuery
import io.github.clasicrando.kdbc.postgresql.result.CopyInResultCollector
import io.github.clasicrando.kdbc.postgresql.result.QueryResultCollector
import io.github.clasicrando.kdbc.postgresql.result.StatementPrepareRequestCollector
import io.github.clasicrando.kdbc.postgresql.statement.PgEncodeBuffer
import io.github.clasicrando.kdbc.postgresql.statement.PgPreparedStatement
import io.github.clasicrando.kdbc.postgresql.stream.PgBlockingStream
import io.github.oshai.kotlinlogging.KLoggingEventBuilder
import io.github.oshai.kotlinlogging.KotlinLogging
import io.github.oshai.kotlinlogging.Level
import kotlinx.atomicfu.AtomicBoolean
import kotlinx.atomicfu.atomic
import kotlinx.atomicfu.locks.reentrantLock
import kotlinx.atomicfu.locks.withLock
import kotlinx.datetime.Clock
import kotlinx.io.Sink
import kotlinx.io.Source
import java.io.ByteArrayOutputStream
import java.io.InputStream
import java.io.OutputStream
import kotlin.reflect.typeOf

private val logger = KotlinLogging.logger {}

/**
 * [BlockingConnection] object for a Postgresql database. A new instance cannot be created but
 * rather the [io.github.clasicrando.kdbc.postgresql.Postgres.blockingConnection] method should be
 * called to receive a new [PgBlockingConnection] ready for user usage. This method will use
 * connection pooling behind the scenes as to reduce unnecessary TCP connection creation to the
 * server when an application creates and closes connections frequently.
 *
 * This type is thread-safe due to an internal [reentrantLock] but that means that only 1 thread
 * can execute operations against the connection at a given time. If another thread attempts to
 * execute a command, the thread will block until the lock is freed.
 */
class PgBlockingConnection internal constructor(
    /** Connection options supplied when requesting a new Postgresql connection */
    internal val connectOptions: PgConnectOptions,
    /** Underlining stream of data to and from the database */
    private val stream: PgBlockingStream,
    /**
     * Reference to the [io.github.clasicrando.kdbc.core.pool.BlockingConnectionPool] that owns
     * this [PgBlockingConnection]
     */
    private val pool: PgBlockingConnectionPool,
    /** Type registry for connection. Used to decode data rows returned by the server. */
    @PublishedApi internal val typeCache: PgTypeCache = pool.typeCache,
) : BlockingConnection, DefaultUniqueResourceId() {
    private val _inTransaction: AtomicBoolean = atomic(false)
    override val inTransaction: Boolean get() = _inTransaction.value

    /** Lock keeping multiple threads from executing calls to the database */
    private val lock = reentrantLock()
    /**
     * Cache of [PgPreparedStatement] where the key is the query that initiated the prepared
     * statement. This is not thread safe, therefore it should only be accessed after querying
     * running has been disabled to ensure a single thread/coroutine is accessing the contents.
     */
    private val preparedStatements: MutableMap<String, PgPreparedStatement> = mutableMapOf()
    /** ID of the next prepared statement executed. Incremented after each statement is created */
    private var nextStatementId = 1

    /**
     * Create a log message at the specified [level], applying the [block] to the
     * [KLogger.at][io.github.oshai.kotlinlogging.KLogger.at] method.
     */
    private inline fun log(level: Level, crossinline block: KLoggingEventBuilder.() -> Unit) {
        logWithResource(logger, level, block)
    }

    /**
     * Verify that the connection is currently active. Throws an [IllegalStateException] is the
     * underlining connection is no longer active. This should be performed before each call to for
     * the [PgBlockingConnection] to interact with the server.
     */
    private fun checkConnected() {
        check(isConnected) { "Cannot execute queries against a closed connection" }
    }

    override val isConnected: Boolean get() = stream.isConnected

    override fun begin() {
        try {
            sendSimpleQuery("BEGIN;")
            if (!_inTransaction.compareAndSet(expect = false, update = true)) {
                throw UnexpectedTransactionState(inTransaction = true)
            }
        } catch (ex: OutOfMemoryError) {
            throw ex
        } catch (ex: UnexpectedTransactionState) {
            throw ex
        } catch (ex: Throwable) {
            if (!_inTransaction.compareAndSet(expect = true, update = false)) {
                try {
                    rollback()
                } catch (ex2: Throwable) {
                    log(Level.WARN) {
                        message = "Error while trying to rollback. BEGIN called while in transaction"
                        cause = ex2
                    }
                }
            }
            throw ex
        }
    }

    override fun commit() {
        try {
            sendSimpleQuery("COMMIT;")
        } finally {
            if (!_inTransaction.compareAndSet(expect = true, update = false)) {
                log(Level.WARN) {
                    this.message = "Attempted to COMMIT a connection not within a transaction"
                }
            }
        }
    }

    override fun rollback() {
        try {
            sendSimpleQuery("ROLLBACK;")
        } finally {
            if (!_inTransaction.compareAndSet(expect = true, update = false)) {
                log(Level.WARN) {
                    this.message = "Attempted to ROLLBACK a connection not within a transaction"
                }
            }
        }
    }

    /**
     * Log a [message] that is processed but ignored since it's not important during the current
     * operation
     */
    private fun logUnexpectedMessage(message: PgMessage): Loop {
        log(Kdbc.detailedLogging) {
            this.message = "Ignoring {message} since it's not an error or the desired type"
            payload = mapOf("message" to message)
        }
        return Loop.Continue
    }

    /**
     * Process the contents of a [PgMessage.ReadyForQuery] message (i.e. a [TransactionStatus])
     * that was received at the end of a flow of query responses.
     */
    private fun handleTransactionStatus(transactionStatus: TransactionStatus) {
        if (transactionStatus == TransactionStatus.FailedTransaction)
        {
            log(Level.WARN) {
                this.message = "Server reported failed transaction"
            }
        }
    }

    /**
     * Collect all [QueryResult]s for the executed [statements] as a single [StatementResult].
     *
     * This iterates over all [statements], updating the [PgPreparedStatement] with the received
     * metadata (if any), collecting each row received, and exiting the current iteration once a
     * query done message is received. The collector only stores a [QueryResult] for the iteration
     * if a [PgMessage.CommandComplete] message is received.
     *
     * If an error message is received during result collection and [isAutoCommit] is false
     * (meaning there is no implicit commit between each statement), the previously received query
     * done message was signifying the end of all results and the [statements] iteration is
     * terminated. If [isAutoCommit] was true, then each result is processed with all errors also
     * collected into a list for evaluation later.
     *
     * Once completing the iteration over [statements] (successfully or with errors), the
     * collection of errors is reduced into one [Throwable] (if any) and thrown. Otherwise, the
     * method exits with all [QueryResult] packed into a single [StatementResult].
     */
    private fun collectResults(
        isAutoCommit: Boolean,
        statements: Array<PgPreparedStatement> = emptyArray(),
    ): StatementResult {
        val queryResultCollector = QueryResultCollector(this, typeCache)
        for (preparedStatement in statements) {
            queryResultCollector.processNextStatement(preparedStatement)
            stream.processMessageLoop(queryResultCollector::processNextMessage)
                .onFailure(queryResultCollector.errors::add)
            if (queryResultCollector.errors.isNotEmpty() && !isAutoCommit) {
                break
            }
        }
        queryResultCollector.transactionStatus?.let(::handleTransactionStatus)

        val error = queryResultCollector.errors.reduceToSingleOrNull()
            ?: return queryResultCollector.buildStatementResult()
        log(Kdbc.detailedLogging) {
            message = "Error during single query execution"
            cause = error
        }
        throw error
    }

    /**
     * Collect all [QueryResult]s for the query as a single [StatementResult].
     *
     * This allows for multiple [QueryResult]s from a single query to be collected by continuously
     * passing backend messages to a [QueryResultCollector] for processing with
     * [QueryResultCollector.processNextMessage].
     *
     * After the query result collection, the collection of errors is checked and aggregated into
     * one [Throwable] (if any) and thrown. Otherwise, the method exits with all [QueryResult]s
     * packed into a single [StatementResult].
     */
    private fun collectResult(
        statement: PgPreparedStatement? = null,
    ): StatementResult {
        val queryResultCollector = QueryResultCollector(this, typeCache)
        queryResultCollector.processNextStatement(statement)
        stream.processMessageLoop(queryResultCollector::processNextMessage)
            .onFailure(queryResultCollector.errors::add)
        queryResultCollector.transactionStatus?.let(::handleTransactionStatus)

        val error = queryResultCollector.errors.reduceToSingleOrNull()
            ?: return queryResultCollector.buildStatementResult()
        log(Kdbc.detailedLogging) {
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
     * **Note**
     *
     * This method will defer to [sendExtendedQuery] if the [query] does not contain a semicolon
     * and the [PgConnectOptions.useExtendedProtocolForSimpleQueries] is true (the default value).
     *
     * [postgres docs](https://www.postgresql.org/docs/current/protocol-flow.html#PROTOCOL-FLOW-SIMPLE-QUERY)
     *
     * @throws IllegalArgumentException if the [query] is blank
     * @throws IllegalStateException if the underlining connection is no longer active
     */
    internal fun sendSimpleQuery(
        query: String,
    ): StatementResult {
        require(query.isNotBlank()) { "Cannot send an empty query" }
        checkConnected()
        if (!query.contains(";") && connectOptions.useExtendedProtocolForSimpleQueries) {
            return sendExtendedQuery(query, emptyList())
        }
        return lock.withLock {
            log(connectOptions.logSettings.statementLevel) {
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
    private fun executeStatementPrepare(
        query: String,
        parameterTypes: List<Int>,
        statement: PgPreparedStatement,
    ) {
        stream.writeManyToStream {
            val parseMessage = PgMessage.Parse(
                preparedStatementName = statement.statementName,
                query = query,
                parameterTypes = parameterTypes,
            )
            yield(parseMessage)
            val describeMessage = PgMessage.Describe(
                target = MessageTarget.PreparedStatement,
                name = statement.statementName,
            )
            yield(describeMessage)
            yield(PgMessage.Sync)
        }

        val prepareRequestCollector = StatementPrepareRequestCollector(this, statement)
        stream.processMessageLoop(prepareRequestCollector::processNextMessage)
            .onFailure(prepareRequestCollector.errors::add)
        prepareRequestCollector.transactionStatus?.let(::handleTransactionStatus)

        val error = prepareRequestCollector.errors.reduceToSingleOrNull() ?: return
        log(Kdbc.detailedLogging) {
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
    private fun removeOldestPreparedStatement() {
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
    private fun getOrCachePreparedStatement(query: String): PgPreparedStatement {
        while (preparedStatements.size >= connectOptions.statementCacheCapacity) {
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
    private fun prepareStatement(
        query: String,
        parameters: List<QueryParameter>,
    ): PgPreparedStatement {
        val statement = getOrCachePreparedStatement(query)

        require(statement.paramCount == parameters.size) {
            """
            Query does not have the correct number of parameters.
            
            ${query.replaceIndent("            ")}
            
            Expected ${statement.paramCount}, got ${parameters.size}
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
     * - [PgMessage.Execute] with the statement name
     * - [PgMessage.Close] with the statement name
     * - If [sendSync] is true, [PgMessage.Sync] is sent
     */
    private fun executePreparedStatement(
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
    internal fun sendExtendedQuery(
        query: String,
        parameters: List<QueryParameter>,
    ): StatementResult {
        require(query.isNotBlank()) { "Cannot send an empty query" }
        checkConnected()
        return lock.withLock {
            val statement = try {
                prepareStatement(query, parameters)
            } catch (ex: Throwable) {
                throw ex
            }

            val encodeBuffer = PgEncodeBuffer(statement.parameterTypeOids, typeCache)
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
    private fun releasePreparedStatement(preparedStatement: PgPreparedStatement) {
        val closeMessage = PgMessage.Close(
            target = MessageTarget.PreparedStatement,
            targetName = preparedStatement.statementName,
        )
        lock.withLock {
            stream.writeManyToStream(closeMessage, PgMessage.Sync)
            stream.waitForOrError<PgMessage.CommandComplete>()
            preparedStatements.remove(preparedStatement.query)
        }
    }

    /**
     * Dispose of all internal connection resources while also sending a [PgMessage.Terminate] so
     * the database server is alerted to the closure.
     */
    internal fun dispose() {
        try {
            if (stream.isConnected) {
                stream.writeToStream(PgMessage.Terminate)
                log(Kdbc.detailedLogging) {
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

    override fun close() {
        if (!pool.giveBack(this@PgBlockingConnection)) {
            dispose()
        }
    }

    /**
     * Allows for vararg specification of prepared statements using [pipelineQueries] where syncAll
     * is the default true. See the other method doc for more information.
     *
     * @see pipelineQueries
     */
    fun pipelineQueriesSyncAll(
        vararg queries: Pair<String, List<QueryParameter>>,
    ): StatementResult {
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
    fun pipelineQueries(
        syncAll: Boolean = true,
        vararg queries: Pair<String, List<QueryParameter>>,
    ): StatementResult = lock.withLock {
        val statements = Array(queries.size) { i ->
            val (queryText, queryParams) = queries[i]
            prepareStatement(query = queryText, parameters = queryParams)
        }
        for ((i, statement) in statements.withIndex()) {
            val encodeBuffer = PgEncodeBuffer(statement.parameterTypeOids, typeCache)
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

    /**
     * Internal method for executing a `COPY IN` command. Steps are:
     *
     * 1. Execute the [copyQuery]
     * 2. Wait for a [PgMessage.CopyInResponse] exiting if a [PgMessage.ErrorResponse] is received
     * 3. Write all elements in the [data] sequence as [PgMessage.CopyData] to the backend
     * 4. Writing a [PgMessage.CopyDone] message to instruct the backend to parse the data sent
     * 5. Collect the result messages using a [CopyInResultCollector]
     * 6. Process the [TransactionStatus] response from the backend
     * 7. Collect errors sent from the server during message collection
     * 8. Return a [QueryResult] with the number of rows impacted and message sent from the backend
     *
     * Any unexpected errors during result collection will be aggregated and thrown before
     * returning. However, if an exception is thrown while sending/creating [PgMessage.CopyData]
     * messages, the expected [PgMessage.ErrorResponse] received from the server will be treated as
     * a result message and not an error.
     */
    private fun copyInInternal(copyQuery: String, data: Sequence<ByteArray>): QueryResult {
        log(connectOptions.logSettings.statementLevel) {
            message = STATEMENT_TEMPLATE
            payload = mapOf("query" to copyQuery)
        }
        stream.writeToStream(PgMessage.Query(copyQuery))
        stream.waitForOrError<PgMessage.CopyInResponse>()

        var wasFailed = false
        var failureReason: Exception? = null
        try {
            stream.writeManySized(data.map { PgMessage.CopyData(it) })
            stream.writeToStream(PgMessage.CopyDone)
        } catch (ex: Exception) {
            failureReason = ex
            if (stream.isConnected) {
                wasFailed = true
                stream.writeToStream(PgMessage.CopyFail("Exception collecting data\nError:\n$ex"))
            }
        }

        val copyInResultCollector = CopyInResultCollector(this, wasFailed)
        failureReason?.let { copyInResultCollector.errors.add(it) }
        stream.processMessageLoop(copyInResultCollector::processMessage)
            .onFailure(copyInResultCollector.errors::add)
        copyInResultCollector.transactionStatus?.let(::handleTransactionStatus)

        val error = copyInResultCollector.errors.reduceToSingleOrNull()
        if (error != null) {
            throw error
        }

        return QueryResult(
            rowsAffected = copyInResultCollector.completeMessage?.rowCount ?: 0,
            message = copyInResultCollector.completeMessage?.message
                ?: "Default copy in complete message",
        )
    }

    /**
     * Execute a `COPY FROM` command using the options supplied in the [copyInStatement] and feed
     * the [data] provided as a [Sequence] to the server. Since the server will parse the bytes
     * supplied as a continuous flow of data rather than records, each item in the sequence does
     * not need to represent a record but, it's usually convenient to parse data as records,
     * convert to a [ByteArray] and feed that through the flow.
     *
     * If the server sends an error message during or at completion of streaming the copy [data],
     * the message will be captured and thrown after completing the COPY process and the connection
     * with the server reverts to regular queries.
     */
    fun copyIn(copyInStatement: CopyStatement.From, data: Sequence<ByteArray>): QueryResult {
        checkConnected()

        val copyQuery = copyInStatement.toQuery()
        return lock.withLock { copyInInternal(copyQuery, data) }
    }

    /**
     * Execute a `COPY FROM` command using the options supplied in the [copyInStatement] and feed
     * the contents of the [source]. The data within the [source] must be a text based (i.e.
     * txt/csv file data).
     *
     * If the server sends an error message during or at completion of streaming the copy [source],
     * the message will be captured and thrown after completing the COPY process and the connection
     * with the server reverts to regular queries.
     *
     * @throws IllegalArgumentException if the [copyInStatement] is not [CopyStatement.CopyText]
     */
    fun copyIn(copyInStatement: CopyStatement.From, source: Source): QueryResult {
        require(copyInStatement is CopyStatement.CopyText)
        return copyIn(
            copyInStatement = copyInStatement,
            data = source.chunkedBytes()
        )
    }

    /**
     * Execute a `COPY FROM` command using the options supplied in the [copyInStatement] and feed
     * the contents of the [inputStream]. The data within the [inputStream] must be a text based
     * (i.e. txt/csv file data).
     *
     * If the server sends an error message during or at completion of streaming the copy
     * [inputStream], the message will be captured and thrown after completing the COPY process and
     * the connection with the server reverts to regular queries.
     *
     * @throws IllegalArgumentException if the [copyInStatement] is not [CopyStatement.CopyText]
     */
    fun copyIn(copyInStatement: CopyStatement.From, inputStream: InputStream): QueryResult {
        require(copyInStatement is CopyStatement.CopyText)
        return copyIn(
            copyInStatement = copyInStatement,
            data = inputStream.chunkedBytes()
        )
    }

    /**
     * Execute a `COPY FROM` command using the options supplied in the [copyInStatement] and feed
     * each [PgCsvCopyRow] supplied to the COPY sink by using the [PgCsvCopyRow.values] as the
     * CSV row contents. By default, the [Any.toString] method is called to convert the data into
     * CSV rows.
     *
     * If the server sends an error message during or at completion of streaming the copy data, the
     * message will be captured and thrown after completing the COPY process and the connection
     * with the server reverts to regular queries.
     *
     * @throws IllegalArgumentException if the [copyInStatement] is not [CopyStatement.TableFromCsv]
     * @throws kotlinx.io.IOException if the file cannot be read due to an IO related issue
     */
    fun copyIn(
        copyInStatement: CopyStatement.TableFromCsv,
        data: Sequence<PgCsvCopyRow>,
    ): QueryResult {
        val outputStream = ByteArrayOutputStream()
        val writer = csvWriter {
            delimiter = copyInStatement.delimiter
            quote {
                char = copyInStatement.quote
            }
            lineTerminator = "\n"
            nullCode = copyInStatement.nullString
        }
        return copyIn(
            copyInStatement = copyInStatement,
            data = data.chunked(size = 50) { chunk ->
                writer.open(outputStream) { writeRows(chunk.asSequence().map { it.values }) }
                val bytes = outputStream.toByteArray()
                outputStream.reset()
                bytes
            }
        )
    }

    /**
     * Execute a `COPY FROM` command using the options supplied in the [copyInStatement] and feed
     * each [PgBinaryCopyRow] supplied to the COPY sink by calling [PgBinaryCopyRow.encodeValues]
     * with a [PgEncodeBuffer] to encode the table rows as binary values.
     *
     * If the server sends an error message during or at completion of streaming the copy data, the
     * message will be captured and thrown after completing the COPY process and the connection
     * with the server reverts to regular queries.
     *
     * @throws IllegalArgumentException if the [copyInStatement] is not [CopyStatement.TableFromCsv]
     * @throws kotlinx.io.IOException if the file cannot be read due to an IO related issue
     */
    fun copyIn(
        copyInStatement: CopyStatement.TableFromBinary,
        data: Sequence<PgBinaryCopyRow>,
    ): QueryResult {
        val schemaName = copyInStatement.schemaName.trim()
        val metadata = createPreparedQuery(CopyTableMetadata.QUERY)
            .bind(copyInStatement.tableName)
            .bind(schemaName)
            .fetchAll(CopyTableMetadata.Companion)
        val fields = metadata.map { it.type.oid }
        val buffer = PgEncodeBuffer(parameterTypeOids = fields, typeCache = typeCache)
        return copyIn(
            copyInStatement = copyInStatement,
            data = sequence<ByteArray> {
                yield(pgBinaryCopyHeader)
                val mappedSequence = data.chunked(size = 50).map { chunk ->
                    for (row in chunk) {
                        buffer.innerBuffer.writeShort(row.valueCount)
                        row.encodeValues(buffer)
                    }
                    buffer.innerBuffer.copyToArray()
                }
                yieldAll(mappedSequence)
                yield(pgBinaryCopyTrailer)
            }
        )
    }

    /**
     * Internal method for executing a `COPY OUT` command. Steps are:
     *
     * 1. Execute the [copyQuery]
     * 2. Wait for a [PgMessage.CopyOutResponse] exiting if a [PgMessage.ErrorResponse] is received
     * 3. Process all incoming messages by yielding a [Sequence] of [ByteArray] instances from
     * [PgMessage.CopyData] messages. Exit the loop when [PgMessage.ReadyForQuery] is received.
     */
    private fun copyOutInternal(copyQuery: String): Sequence<ByteArray> {
        log(connectOptions.logSettings.statementLevel) {
            message = STATEMENT_TEMPLATE
            payload = mapOf("query" to copyQuery)
        }

        stream.writeToStream(PgMessage.Query(copyQuery))
        stream.waitForOrError<PgMessage.CopyOutResponse>()
        return sequence {
            stream.processMessageLoop { message ->
                when (message) {
                    is PgMessage.ErrorResponse -> {
                        throw GeneralPostgresError(message)
                    }
                    is PgMessage.CopyData -> {
                        yield(message.data)
                        Loop.Continue
                    }
                    is PgMessage.CopyDone, is PgMessage.CommandComplete -> Loop.Continue
                    is PgMessage.ReadyForQuery -> {
                        handleTransactionStatus(message.transactionStatus)
                        Loop.Break
                    }
                    else -> logUnexpectedMessage(message)
                }
            }.getOrThrow()
        }
    }

    /**
     * Execute a `COPY TO` command using the options supplied in the [copyOutStatement], reading
     * each `CopyData` server response message and passing the data through the returned
     * [Sequence]. Since [Sequence]'s do not buffer results, the caller of this methods should
     * collect or iterate over the [Sequence] as soon as possible to not hold up the connection
     */
    fun copyOut(
        copyOutStatement: CopyStatement.To,
    ): QueryResult {
        checkConnected()

        val copyQuery = copyOutStatement.toQuery()
        val fields = when (copyOutStatement) {
            is CopyStatement.CopyTable -> {
                val schemaName = copyOutStatement.schemaName.trim()
                val metadata = createPreparedQuery(CopyTableMetadata.QUERY)
                    .bind(copyOutStatement.tableName)
                    .bind(schemaName)
                    .fetchAll(CopyTableMetadata.Companion)
                CopyTableMetadata.getFields(copyOutStatement.format, metadata)
            }
            is CopyStatement.CopyQuery -> {
                val statement = prepareStatement(copyOutStatement.query, emptyList())
                statement.resultMetadata
            }
            else -> error("Received an invalid `CopyStatement.To`. This should never happen")
        }

        return lock.withLock {
            val sequence = copyOutInternal(copyQuery)
            CopyOutCollector(copyOutStatement, fields)
                .collectBlocking(this, sequence)
        }
    }

    /**
     * Execute a `COPY TO` command using the options supplied in the [copyOutStatement], writing
     * each row returned from the query to the [sink] supplied
     */
    fun copyOut(copyOutStatement: CopyStatement.To, sink: Sink) {
        checkConnected()

        val copyQuery = copyOutStatement.toQuery()
        lock.withLock {
            copyOutInternal(copyQuery).forEach(sink::write)
        }
    }

    /**
     * Execute a `COPY TO` command using the options supplied in the [copyOutStatement], writing
     * each row returned from the query to the [outputStream] supplied
     */
    fun copyOut(copyOutStatement: CopyStatement.To, outputStream: OutputStream) {
        checkConnected()

        val copyQuery = copyOutStatement.toQuery()
        lock.withLock {
            copyOutInternal(copyQuery).forEach(outputStream::write)
        }
    }

    /**
     * Execute a `LISTEN` command for the specified [channelName]. Allows this connection to
     * receive notifications sent to this connection's current database. All received messages
     * are accessible from the [getNotifications] method.
     */
    fun listen(channelName: String) {
        val query = "LISTEN ${channelName.quoteIdentifier()};"
        sendSimpleQuery(query)
    }

    /**
     * Execute a `NOTIFY` command for the specified [channelName] with the supplied [payload]. This
     * sends a notification to any connection connected to this connection's current database.
     */
    fun notify(channelName: String, payload: String) {
        val escapedPayload = payload.replace("'", "''")
        sendSimpleQuery("NOTIFY ${channelName.quoteIdentifier()}, '${escapedPayload}';")
    }

    /**
     * Returns all available [PgNotification]s from the message stream. Flushes the stream to
     * obtain all pending messages sent from the server then pull every message from the
     * notification queue.
     */
    fun getNotifications(): List<PgNotification> {
        stream.flushMessages()
        return generateSequence { stream.notificationsQueue.poll() }.toList()
    }

    /**
     * Register an [Enum] class as a new type available for encoding and decoding.
     *
     * @param type name of the type in the database (optionally schema qualified if not in public
     * schema)
     */
    inline fun <reified E : Enum<E>> registerEnumType(type: String) {
        typeCache.addEnumType(
            connection = this,
            name = type,
            kType = typeOf<E>(),
            enumValues = enumValues<E>(),
        )
    }

    /**
     * Register [T] as a new type composite type available for encoding and decoding.
     *
     * @param type name of the type in the database (optionally schema qualified if not in public
     * schema)
     */
    inline fun <reified T : Any> registerCompositeType(
        type: String,
        compositeTypeDefinition: CompositeTypeDefinition<T>? = null,
    ) {
        typeCache.addCompositeType(
            connection = this,
            name = type,
            cls = T::class,
            compositeTypeDefinition = compositeTypeDefinition,
        )
    }

    override fun createQuery(query: String): BlockingQuery = PgBlockingQuery(this, query)

    override fun createPreparedQuery(query: String): BlockingPreparedQuery {
        return PgBlockingPreparedQuery(this, query)
    }

    override fun createPreparedQueryBatch(): BlockingPreparedQueryBatch {
        return PgBlockingPreparedQueryBatch(this)
    }

    companion object {
        private const val STATEMENT_TEMPLATE = "Sending {query}"

        /**
         * Create a new [PgBlockingConnection] instance using the supplied [connectOptions],
         * [stream] and [pool] (the pool that owns this connection).
         */
        internal fun connect(
            connectOptions: PgConnectOptions,
            stream: PgBlockingStream,
            pool: PgBlockingConnectionPool,
        ): PgBlockingConnection {
            var connection: PgBlockingConnection? = null
            try {
                connection = PgBlockingConnection(connectOptions, stream, pool)
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
