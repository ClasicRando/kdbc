package io.github.clasicrando.kdbc.postgresql.connection

import io.github.clasicrando.kdbc.core.Loop
import io.github.clasicrando.kdbc.core.buffer.ByteReadBuffer
import io.github.clasicrando.kdbc.core.cache.LruCache
import io.github.clasicrando.kdbc.core.chunked
import io.github.clasicrando.kdbc.core.chunkedBytes
import io.github.clasicrando.kdbc.core.config.Kdbc
import io.github.clasicrando.kdbc.core.connection.AbstractConnection
import io.github.clasicrando.kdbc.core.connection.Connection
import io.github.clasicrando.kdbc.core.exceptions.KdbcException
import io.github.clasicrando.kdbc.core.logWithResource
import io.github.clasicrando.kdbc.core.normalizeWhitespace
import io.github.clasicrando.kdbc.core.parseBytesAsCsvRow
import io.github.clasicrando.kdbc.core.query.Query
import io.github.clasicrando.kdbc.core.query.QueryParameter
import io.github.clasicrando.kdbc.core.query.RowParser
import io.github.clasicrando.kdbc.core.query.bind
import io.github.clasicrando.kdbc.core.query.execute
import io.github.clasicrando.kdbc.core.query.fetchAll
import io.github.clasicrando.kdbc.core.query.fetchScalar
import io.github.clasicrando.kdbc.core.query.query
import io.github.clasicrando.kdbc.core.result.DataRow
import io.github.clasicrando.kdbc.core.result.Either
import io.github.clasicrando.kdbc.core.result.QueryResult
import io.github.clasicrando.kdbc.core.result.getAsNonNull
import io.github.clasicrando.kdbc.core.splitQuery
import io.github.clasicrando.kdbc.core.statement.CsvDataRow
import io.github.clasicrando.kdbc.core.statement.mapIntoCsvByteArrayChunks
import io.github.clasicrando.kdbc.postgresql.column.PgColumnDescription
import io.github.clasicrando.kdbc.postgresql.column.PgFormatCode
import io.github.clasicrando.kdbc.postgresql.column.PgValue
import io.github.clasicrando.kdbc.postgresql.copy.CopyHeader
import io.github.clasicrando.kdbc.postgresql.copy.CopyStatement
import io.github.clasicrando.kdbc.postgresql.copy.CopyTableMetadata
import io.github.clasicrando.kdbc.postgresql.copy.PgBinaryCopyRow
import io.github.clasicrando.kdbc.postgresql.copy.PgCopyEncodeBuffer
import io.github.clasicrando.kdbc.postgresql.exceptions.PgException
import io.github.clasicrando.kdbc.postgresql.message.MessageTarget
import io.github.clasicrando.kdbc.postgresql.message.PgMessage
import io.github.clasicrando.kdbc.postgresql.message.TransactionStatus
import io.github.clasicrando.kdbc.postgresql.pool.PgConnectionPool
import io.github.clasicrando.kdbc.postgresql.result.PgDataRow
import io.github.clasicrando.kdbc.postgresql.statement.PgArgument
import io.github.clasicrando.kdbc.postgresql.statement.PgPreparedStatement
import io.github.clasicrando.kdbc.postgresql.stream.PgStream
import io.github.clasicrando.kdbc.postgresql.type.BaseCompositeTypeDescription
import io.github.clasicrando.kdbc.postgresql.type.CompositeTypeDefinition
import io.github.clasicrando.kdbc.postgresql.type.EnumTypeDescription
import io.github.clasicrando.kdbc.postgresql.type.PgType
import io.github.clasicrando.kdbc.postgresql.type.PgTypeCache
import io.github.clasicrando.kdbc.postgresql.type.PgTypeDescription
import io.github.clasicrando.kdbc.postgresql.type.ReflectionCompositeTypeDescription
import io.github.clasicrando.kdbc.postgresql.type.ValueTypeDescription
import io.github.oshai.kotlinlogging.KLoggingEventBuilder
import io.github.oshai.kotlinlogging.KotlinLogging
import io.github.oshai.kotlinlogging.Level
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.FlowCollector
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.emitAll
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.mapNotNull
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.io.Sink
import kotlinx.io.Source
import kotlinx.io.asSink
import kotlinx.io.asSource
import kotlinx.io.buffered
import kotlinx.io.readByteArray
import java.io.InputStream
import java.io.OutputStream
import kotlin.reflect.KClass
import kotlin.reflect.KType
import kotlin.reflect.full.primaryConstructor
import kotlin.reflect.typeOf

private val logger = KotlinLogging.logger {}

/**
 * [Connection] object for a Postgresql database. A new instance cannot be created but rather the
 * [io.github.clasicrando.kdbc.postgresql.Postgres.connection] method should be called to receive a
 * new [PgConnection] ready for usage. This method will use connection pooling behind the scenes as
 * to reduce unnecessary TCP connection creation to the server when an application creates and
 * closes connections frequently.
 */
public class PgConnection
internal constructor(
    /** Connection options supplied when requesting a new Postgresql connection */
    internal val connectOptions: PgConnectOptions,
    /** Underlining stream of data to and from the database */
    internal val stream: PgStream,
    /** Reference to the connection pool that owns this connection */
    internal val pool: PgConnectionPool,
    /** Type registry for connection. Used to decode data rows returned by the server. */
    @PublishedApi internal val typeCache: PgTypeCache = pool.typeCache,
) : AbstractConnection() {
    private var pendingReaderForQueryCount = 0

    /**
     * Suspending [Mutex] to allow only 1 coroutine to execute queries against this connection. Each
     * query operation is wrapped in a [Mutex.withLock] to ensure fair but exclusive access to the
     * connection.
     */
    private val mutex = Mutex()

    /**
     * Cache of [PgPreparedStatement] where the key is the query that initiated the prepared
     * statement. This is not thread safe, therefore it should only be accessed after querying
     * running has been disabled to ensure a single thread/coroutine is accessing the contents.
     */
    private val preparedStatements: LruCache<String, PgPreparedStatement> =
        LruCache(connectOptions.statementCacheCapacity)

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
     * the connection to interact with the server.
     */
    private fun checkConnected() {
        check(isConnected) { "Cannot execute queries against a closed connection" }
    }

    override val isConnected: Boolean
        get() = stream.isConnected

    override suspend fun isValid(): Boolean {
        if (!isConnected) {
            return false
        }
        return mutex.withLock {
            try {
                waitUntilReady()
                stream.writeToStream(PgMessage.Query(""))
                stream.processMessageLoop(throwOnError = false) { message ->
                    when (message) {
                        is PgMessage.ErrorResponse -> return false
                        is PgMessage.ReadyForQuery -> Loop.Break
                        else -> Loop.Continue
                    }
                }
            } catch (_: KdbcException) {
                return false
            }
            return true
        }
    }

    override suspend fun close() {
        if (!pool.giveBack(this)) {
            dispose()
        }
    }

    override suspend fun executeQuery(query: Query): Flow<Either<QueryResult, DataRow>> {
        log(connectOptions.statementLogLevel) {
            message = "Sending query: ${query.sql.normalizeWhitespace()}"
        }
        if (query.parameters.isEmpty()) {
            return sendSimpleQuery(query.sql)
        }
        return sendExtendedQuery(query.sql, query.parameters)
    }

    override suspend fun executeQueryBatch(
        batch: List<Query>,
        withinTransaction: Boolean,
    ): Flow<Either<QueryResult, DataRow>> {
        if (withinTransaction && inTransaction) {
            throw PgException(
                "Cannot sent query batch within transaction if a transaction is already open"
            )
        }
        return pipelineQueries(syncAll = !withinTransaction, queries = batch)
    }

    /**
     * Log a [message] that is processed but ignored since it's not important during the current
     * operation
     */
    private fun logUnexpectedMessage(message: PgMessage): Loop {
        log(Kdbc.detailedLogging) {
            this.message = "Ignoring $message since it's not an error or the desired type"
        }
        return Loop.Continue
    }

    /**
     * Process the contents of a [PgMessage.ReadyForQuery] message (i.e. a [TransactionStatus]) that
     * was received at the end of a flow of query responses.
     */
    private fun handleReadyForQuery(readyForQuery: PgMessage.ReadyForQuery) {
        if (--pendingReaderForQueryCount < 0) {
            log(Level.WARN) { message = "Received more ReadyForQuery than expected" }
            pendingReaderForQueryCount = 0
        }
        if (readyForQuery.transactionStatus == TransactionStatus.FailedTransaction) {
            log(Level.WARN) { this.message = "Server reported failed transaction." }
        }
    }

    private suspend fun waitUntilReady() {
        while (pendingReaderForQueryCount > 0) {
            val message = stream.waitForOrError<PgMessage.ReadyForQuery>()
            handleReadyForQuery(message)
        }
    }

    private suspend fun writeSync() {
        stream.writeToStream(PgMessage.Sync)
        pendingReaderForQueryCount++
    }

    /**
     * Collect all results for the query as a single [Flow] of [QueryResult] and [DataRow].
     *
     * After the query result collection, the collection of errors is checked and aggregated into
     * one [Throwable] (if any) and thrown.
     */
    private suspend fun FlowCollector<Either<QueryResult, DataRow>>.collectResult(
        statement: PgPreparedStatement? = null
    ) {
        var columnMapping = statement?.resultMetadata ?: emptyList()
        stream.processMessageLoop { message ->
            when (message) {
                is PgMessage.RowDescription -> {
                    statement?.resultMetadata = message.fields
                    columnMapping = message.fields
                    Loop.Continue
                }
                is PgMessage.DataRow -> {
                    val row =
                        PgDataRow.fromBuffer(
                            buffer = message.rowBuffer,
                            columnMapping = columnMapping,
                            typeCache = typeCache,
                        )
                    emit(Either.Right(row))
                    Loop.Continue
                }
                is PgMessage.CommandComplete -> {
                    val queryResult = QueryResult(message.rowCount, message.message)
                    emit(Either.Left(queryResult))
                    Loop.Continue
                }
                is PgMessage.ReadyForQuery -> {
                    handleReadyForQuery(message)
                    Loop.Break
                }
                is PgMessage.BindComplete,
                is PgMessage.ParseComplete,
                is PgMessage.ParameterDescription,
                is PgMessage.NoData,
                is PgMessage.CloseComplete -> Loop.Continue
                else -> logUnexpectedMessage(message)
            }
        }
    }

    /**
     * Send a query to the postgres database using the simple query protocol. This sends a single
     * [PgMessage.Query] message with the raw SQL query with no parameters. The database then
     * responds with zero or more results that are captured as a [Flow] of zero or more [DataRow]s
     * followed by a [QueryResult] to denote the end of a result.
     *
     * **Note**
     *
     * This method will defer to [sendExtendedQuery] if the [query] does not contain a semicolon and
     * the [PgConnectOptions.useExtendedProtocolForSimpleQueries] is true (the default value).
     *
     * [postgres
     * docs](https://www.postgresql.org/docs/current/protocol-flow.html#PROTOCOL-FLOW-SIMPLE-QUERY)
     *
     * @throws IllegalArgumentException if the [query] is blank
     * @throws IllegalStateException if the underlining connection is no longer active
     */
    internal suspend fun sendSimpleQuery(query: String): Flow<Either<QueryResult, DataRow>> {
        require(query.isNotBlank()) { "Cannot send an empty query" }
        checkConnected()

        if (connectOptions.useExtendedProtocolForSimpleQueries && !query.contains('$')) {
            val queries = splitQuery(query)
            if (queries.size == 1) {
                return sendExtendedQuery(query, emptyList())
            }
        }

        return mutex.withLock {
            waitUntilReady()
            stream.writeToStream(PgMessage.Query(query))
            pendingReaderForQueryCount++

            flow { collectResult(statement = null) }
        }
    }

    /** Create a [PgPreparedStatement] using the [query] and [parameterTypes] */
    private suspend fun executeStatementPrepare(
        query: String,
        parameterTypes: List<Int>,
    ): PgPreparedStatement {
        val statement = PgPreparedStatement(query, nextStatementId++)
        stream.writeManyToStream(
            PgMessage.Parse(
                preparedStatementName = statement.statementName,
                query = query,
                parameterTypes = parameterTypes,
            ),
            PgMessage.Describe(
                target = MessageTarget.PreparedStatement,
                name = statement.statementName,
            ),
        )
        writeSync()
        stream.processMessageLoop { message ->
            when (message) {
                is PgMessage.ParseComplete,
                is PgMessage.ParameterDescription -> Loop.Continue
                is PgMessage.RowDescription -> {
                    statement.resultMetadata = message.fields.map(PgColumnDescription::withBinary)
                    Loop.Continue
                }
                is PgMessage.NoData -> {
                    statement.resultMetadata = emptyList()
                    Loop.Continue
                }
                is PgMessage.ReadyForQuery -> {
                    handleReadyForQuery(message)
                    Loop.Break
                }
                else -> logUnexpectedMessage(message)
            }
        }
        return statement
    }

    /**
     * Fetch a [PgPreparedStatement] from the cache for the provided [query], returning the
     * statement. If a statement does not already exist for the [query] then a new statement is
     * created in a non-prepared state. If the statement has not already been prepared then
     * [executePreparedStatement] is called to populate the [PgPreparedStatement] with the required
     * data.
     *
     * @throws IllegalArgumentException if the number of [args] does not match the number of
     *   parameters required by the query
     */
    private suspend fun getOrPrepareStatement(
        query: String,
        args: List<PgArgument>,
    ): PgPreparedStatement {
        preparedStatements[query]?.let {
            return it
        }

        val statement =
            executeStatementPrepare(
                query = query,
                parameterTypes =
                    args.map {
                        if (it.parameter == null) {
                            return@map PgType.UNSPECIFIED
                        }
                        it.pgTypeDescription.getActualType(it.parameter).oid
                    },
            )
        preparedStatements.insert(query, statement)?.let { releasePreparedStatement(it.value) }
        return statement
    }

    /**
     * Send all required messages to the server for prepared [statement] execution.
     * - [PgMessage.Bind] with statement name + current [parameters]
     * - [PgMessage.Execute] with the statement name
     * - [PgMessage.Close] with the statement name
     * - If [sendSync] is true, [PgMessage.Sync] is sent
     */
    private suspend fun executePreparedStatement(
        statement: PgPreparedStatement,
        parameters: List<PgArgument>,
        sendSync: Boolean = true,
    ) {
        stream.writeManyToStream(
            PgMessage.Bind(
                portal = null,
                statementName = statement.statementName,
                arguments = parameters,
            ),
            PgMessage.Execute(portalName = null, maxRowCount = 0),
            PgMessage.Close(MessageTarget.Portal, null),
        )
        if (sendSync) {
            writeSync()
        }
    }

    /**
     * Send a query to the postgres database using the extended query protocol. This goes through
     * the process of preparing the query (see [getOrPrepareStatement]) before then executing with
     * the provided parameters. The database then responds with zero or more results that are
     * captured as a [Flow] of zero or more [DataRow]s followed by a [QueryResult] to denote the end
     * of a result.
     *
     * [postgres
     * docs](https://www.postgresql.org/docs/current/protocol-flow.html#PROTOCOL-FLOW-EXT-QUERY)
     *
     * @throws IllegalArgumentException if the [query] is blank
     * @throws IllegalStateException if the underlining connection is no longer active
     */
    internal suspend fun sendExtendedQuery(
        query: String,
        parameters: List<QueryParameter>,
    ): Flow<Either<QueryResult, DataRow>> {
        require(query.isNotBlank()) { "Cannot send an empty query" }
        checkConnected()

        val args = mapParameters(parameters)
        return mutex.withLock {
            waitUntilReady()
            val statement = getOrPrepareStatement(query, args)
            executePreparedStatement(statement, args)
            flow { collectResult(statement = statement) }
        }
    }

    /**
     * Send a [PgMessage.Close] message for the [preparedStatement]. This will close the server side
     * prepared statement and then remove the [preparedStatement] for the client cache.
     */
    private suspend fun releasePreparedStatement(preparedStatement: PgPreparedStatement) {
        stream.writeToStream(
            PgMessage.Close(
                target = MessageTarget.PreparedStatement,
                targetName = preparedStatement.statementName,
            )
        )
        writeSync()
        waitUntilReady()
    }

    /**
     * Dispose of all internal connection resources while also sending a [PgMessage.Terminate] so
     * the database server is alerted to the closure.
     */
    internal suspend fun dispose() {
        try {
            if (stream.isConnected) {
                stream.writeToStream(PgMessage.Terminate)
                log(Kdbc.detailedLogging) { this.message = "Successfully sent termination message" }
            }
        } catch (ex: Exception) {
            log(Level.WARN) {
                this.message = "Error sending terminate message"
                cause = ex
            }
        } finally {
            stream.close()
        }
        preparedStatements.clear()
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
     * implementation of query pipelining. The client only has the ability to send a sync message
     * (instructs the server to autocommit unless the connection is already in an open transaction)
     * after every query execution or 1 sync message after all queries. The default behaviour is to
     * send a sync after each message but that can be overridden by settings [syncAll] to false. If
     * you are sure each one of your statements do not impact each other and can be handled in
     * separate transactions, keep the [syncAll] as default and catch exception thrown during query
     * execution. If the queries you are executing are dependent on each other (e.g. inserting to
     * parent table then child table with a foreign key) then you should override [syncAll] to be
     * false so after 1 query fails, all subsequent queries are ignored. Alternatively, you can also
     * manually begin a transaction using [begin] and handle the transaction state of your
     * connection yourself. In that case, any sync message sent to the server does not cause
     * implicit transactional behaviour.
     *
     * If you are unsure of how this works or what the implications of pipelining has on your
     * database, you should opt to either send multiple statements in separate calls to
     * [sendExtendedQuery] or package your queries into a stored procedure.
     */
    internal suspend fun pipelineQueries(
        syncAll: Boolean,
        queries: List<Query>,
    ): Flow<Either<QueryResult, DataRow>> {
        return mutex.withLock {
            waitUntilReady()
            val statements =
                Array(queries.size) { i ->
                    val query = queries[i]
                    val args = mapParameters(query.parameters)
                    val statement = getOrPrepareStatement(query = query.sql, args = args)
                    statement to args
                }
            for (i in statements.indices) {
                val (statement, args) = statements[i]
                executePreparedStatement(
                    statement = statement,
                    parameters = args,
                    sendSync = syncAll || i == queries.size - 1,
                )
            }
            flow {
                for ((statement, _) in statements) {
                    collectResult(statement = statement)
                }
            }
        }
    }

    /**
     * Internal method for executing a `COPY IN` command. Steps are:
     * 1. Execute the [copyQuery]
     * 2. Wait for a [PgMessage.CopyInResponse] exiting if a [PgMessage.ErrorResponse] is received
     * 3. Write all elements in the [data] sequence as [PgMessage.CopyClientData] to the backend
     * 4. Writing a [PgMessage.CopyDone] message to instruct the backend to parse the data sent
     * 5. Collect the result messages
     * 6. Process the [TransactionStatus] response from the backend
     * 7. Collect errors sent from the server during message collection
     * 8. Return a [QueryResult] with the number of rows impacted and message sent from the backend
     *
     * Any unexpected errors during result collection will be aggregated and thrown before
     * returning. However, if an exception is thrown while sending/creating
     * [PgMessage.CopyClientData] messages, the expected [PgMessage.ErrorResponse] received from the
     * server will be treated as a result message and not an error.
     */
    private suspend fun copyInInternal(copyQuery: String, data: Flow<ByteArray>): QueryResult {
        waitUntilReady()
        log(connectOptions.statementLogLevel) {
            message = "Sending query: ${copyQuery.normalizeWhitespace()}"
        }
        stream.writeToStream(PgMessage.Query(copyQuery))
        stream.waitForOrError<PgMessage.CopyInResponse>()
        pendingReaderForQueryCount++

        try {
            stream.writeManyToStream(data.map { PgMessage.CopyClientData(it) })
            stream.writeToStream(PgMessage.CopyDone)
        } catch (ex: Exception) {
            if (stream.isConnected) {
                stream.writeToStream(PgMessage.CopyFail("Exception collecting data\nError:\n$ex"))
            }
            throw ex
        }

        var completeMessage: PgMessage.CommandComplete? = null
        stream.processMessageLoop { message ->
            when (message) {
                is PgMessage.CommandComplete -> {
                    completeMessage = message
                    Loop.Continue
                }
                is PgMessage.ReadyForQuery -> {
                    handleReadyForQuery(message)
                    Loop.Break
                }
                else -> logUnexpectedMessage(message)
            }
        }

        return QueryResult(
            rowsAffected = completeMessage?.rowCount ?: 0,
            message = completeMessage?.message ?: "Default copy in complete message",
        )
    }

    /**
     * Execute a `COPY FROM` command using the options supplied in the [copyInStatement] and feed
     * the [data] provided as a [Flow] to the server. Since the server will parse the bytes supplied
     * as a continuous flow of data rather than records, each item in the flow does not need to
     * represent a record but, it's usually convenient to parse data as records, convert to a
     * [ByteArray] and feed that through the flow.
     *
     * If the server sends an error message during or at completion of streaming the copy [data],
     * the message will be captured and thrown after completing the COPY process and the connection
     * with the server reverts to regular queries.
     */
    public suspend fun copyIn(
        copyInStatement: CopyStatement.From,
        data: Flow<ByteArray>,
    ): QueryResult {
        checkConnected()

        val copyQuery = copyInStatement.toQuery()
        return mutex.withLock { copyInInternal(copyQuery, data) }
    }

    /**
     * Execute a `COPY FROM` command using the options supplied in the [copyInStatement] and feed
     * the contents of the [source]. The data within the [source] must be a text based (i.e. txt/csv
     * file data).
     *
     * If the server sends an error message during or at completion of streaming the copy [source],
     * the message will be captured and thrown after completing the COPY process and the connection
     * with the server reverts to regular queries.
     *
     * @throws IllegalArgumentException if the [copyInStatement] is not [CopyStatement.CopyText]
     */
    public suspend fun copyIn(copyInStatement: CopyStatement.From, source: Source): QueryResult {
        require(copyInStatement is CopyStatement.CopyText)
        return copyIn(copyInStatement = copyInStatement, data = source.chunkedBytes().asFlow())
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
    public suspend fun copyIn(
        copyInStatement: CopyStatement.From,
        inputStream: InputStream,
    ): QueryResult {
        require(copyInStatement is CopyStatement.CopyText)
        return copyIn(copyInStatement = copyInStatement, source = inputStream.asSource().buffered())
    }

    /**
     * Execute a `COPY FROM` command using the options supplied in the [copyInStatement] and feed
     * each [CsvDataRow] supplied to the COPY sink by using the [CsvDataRow.values] as the CSV row
     * contents. By default, the [Any.toString] method is called to convert the data into CSV rows.
     *
     * If the server sends an error message during or at completion of streaming the copy data, the
     * message will be captured and thrown after completing the COPY process and the connection with
     * the server reverts to regular queries.
     *
     * @throws IllegalArgumentException if the [copyInStatement] is not [CopyStatement.TableFromCsv]
     */
    public suspend fun copyIn(
        copyInStatement: CopyStatement.TableFromCsv,
        data: Flow<CsvDataRow>,
    ): QueryResult {
        return copyIn(
            copyInStatement =
                copyInStatement.copy(
                    delimiter = ',',
                    quote = '"',
                    header = null,
                    default = null,
                    nullString = "",
                    escape = '"',
                ),
            data = data.mapIntoCsvByteArrayChunks(chunkSize = CSV_ROW_BUFFER_SIZE),
        )
    }

    /**
     * Execute a `COPY FROM` command using the options supplied in the [copyInStatement] and feed
     * each [PgBinaryCopyRow] supplied to the COPY sink by calling [PgBinaryCopyRow.encodeValues]
     * with a [PgCopyEncodeBuffer] to encode the table rows as binary values.
     *
     * If the server sends an error message during or at completion of streaming the copy data, the
     * message will be captured and thrown after completing the COPY process and the connection with
     * the server reverts to regular queries.
     *
     * @throws IllegalArgumentException if the [copyInStatement] is not [CopyStatement.TableFromCsv]
     */
    public suspend fun copyIn(
        copyInStatement: CopyStatement.TableFromBinary,
        data: Flow<PgBinaryCopyRow>,
    ): QueryResult {
        val buffer = PgCopyEncodeBuffer(typeCache = typeCache)
        return this.copyIn(
            copyInStatement = copyInStatement,
            data =
                flow<ByteArray> {
                    emit(pgBinaryCopyHeader)
                    val mappedFlow =
                        data.chunked(size = CSV_ROW_BUFFER_SIZE).map { chunk ->
                            for (row in chunk) {
                                buffer.innerBuffer.writeShort(row.valueCount)
                                row.encodeValues(buffer)
                            }
                            buffer.innerBuffer.readByteArray()
                        }
                    emitAll(mappedFlow)
                    emit(pgBinaryCopyTrailer)
                },
        )
    }

    /**
     * Internal method for executing a `COPY OUT` command. Steps are:
     * 1. Execute the [copyQuery]
     * 2. Wait for a [PgMessage.CopyOutResponse] exiting if a [PgMessage.ErrorResponse] is received
     * 3. Process all incoming messages by yielding a [Sequence] of [ByteArray] instances from
     *    [PgMessage.CopyData] messages. Exit the loop when [PgMessage.ReadyForQuery] is received.
     */
    private suspend fun copyOutInternal(copyQuery: String): Flow<ByteReadBuffer> {
        waitUntilReady()
        log(connectOptions.statementLogLevel) {
            message = "Sending query: ${copyQuery.normalizeWhitespace()}"
        }
        stream.writeToStream(PgMessage.Query(copyQuery))
        stream.waitForOrError<PgMessage.CopyOutResponse>()
        pendingReaderForQueryCount++

        return flow {
            stream.processMessageLoop { message ->
                when (message) {
                    is PgMessage.CopyServerData -> {
                        emit(message.data)
                        Loop.Continue
                    }
                    is PgMessage.CopyDone,
                    is PgMessage.CommandComplete -> Loop.Continue
                    is PgMessage.ReadyForQuery -> {
                        handleReadyForQuery(message)
                        Loop.Break
                    }
                    else -> logUnexpectedMessage(message)
                }
            }
        }
    }

    public suspend fun copyOut(copyOutStatement: CopyStatement.To): Flow<ByteReadBuffer> {
        checkConnected()
        return mutex.withLock { copyOutInternal(copyOutStatement.toQuery()) }
    }

    /**
     * Execute a `COPY TO` command using the options supplied in the [copyOutStatement], writing
     * each row returned from the query to the [sink] supplied
     */
    public suspend fun copyOut(copyOutStatement: CopyStatement.To, sink: Sink) {
        copyOut(copyOutStatement).collect { it.transferToSink(sink) }
    }

    /**
     * Execute a `COPY TO` command using the options supplied in the [copyOutStatement], writing
     * each row returned from the query to the [outputStream] supplied
     */
    public suspend fun copyOut(copyOutStatement: CopyStatement.To, outputStream: OutputStream) {
        return copyOut(copyOutStatement = copyOutStatement, sink = outputStream.asSink().buffered())
    }

    /**
     * Execute a `COPY TO` command using the options supplied in the [copyOutStatement], reading
     * each `CopyData` server response message and passing the data through the returned [Flow]. The
     * returned [Flow] is cold so if you want to avoid suspending the server message processor, you
     * should always try to process each item as soon as possible or collect the elements into a
     * [List].
     *
     * Although it's possible to specify the CSV/Text formatting options, because this method
     * doesn't expose the data received from the server, all those options are defaulted to simplify
     * handling on fetched data on the client side. This should not impact your experience executing
     * this command so you can simplify your work by not supplying those parameters in the
     * [CopyStatement.To] constructors.
     */
    public suspend fun copyOutRows(copyOutStatement: CopyStatement.To): Flow<DataRow> {
        val modifiedCopyStatement =
            when (copyOutStatement) {
                is CopyStatement.QueryToBinary,
                is CopyStatement.TableToBinary -> copyOutStatement
                is CopyStatement.QueryToCsv,
                is CopyStatement.QueryToText ->
                    CopyStatement.QueryToCsv(
                        query = copyOutStatement.query,
                        header = CopyHeader.False,
                        nullString = "\\N",
                    )
                is CopyStatement.TableToCsv,
                is CopyStatement.TableToText ->
                    CopyStatement.TableToCsv(
                        schemaName = copyOutStatement.schemaName,
                        tableName = copyOutStatement.tableName,
                        columnNames = copyOutStatement.columnNames,
                        header = CopyHeader.False,
                        nullString = "\\N",
                    )
            }
        val fields =
            when (modifiedCopyStatement) {
                is CopyStatement.CopyTable -> {
                    val schemaName = modifiedCopyStatement.schemaName.trim()
                    val metadata =
                        query(CopyTableMetadata.QUERY)
                            .bind(modifiedCopyStatement.tableName)
                            .bind(schemaName)
                            .fetchAll(this, CopyTableMetadata.Companion)
                    CopyTableMetadata.getFields(modifiedCopyStatement.format, metadata)
                }
                is CopyStatement.CopyQuery -> {
                    val statement =
                        mutex.withLock {
                            val statement =
                                getOrPrepareStatement(modifiedCopyStatement.query, emptyList())
                            releasePreparedStatement(statement)
                            statement
                        }
                    statement.resultMetadata
                }
                else ->
                    throw PgException(
                        "Received an invalid `CopyStatement.To`. This should never happen"
                    )
            }

        var rowCount = 0L
        val flow = copyOut(modifiedCopyStatement)
        return when (modifiedCopyStatement) {
            is CopyStatement.CopyText -> {
                flow.map {
                    val row = parseBytesAsCsvRow(bytes = it, expectedColumnCount = fields.size)
                    PgDataRow(
                        pgValues =
                            Array(row.size) { i ->
                                val rowData = row[i] ?: return@Array null
                                val fieldData = fields[i]
                                PgValue.Text(rowData, fieldData)
                            },
                        columnMapping = fields,
                        typeCache = typeCache,
                    )
                }
            }
            else ->
                flow.mapNotNull { row ->
                    val buffer = getBinaryBuffer(rowCount = ++rowCount, row = row)
                    if (buffer.readShort().toInt() == -1) {
                        return@mapNotNull null
                    }
                    buffer.reset()

                    PgDataRow.fromBuffer(
                        buffer = buffer,
                        columnMapping = fields,
                        typeCache = typeCache,
                    )
                }
        }
    }

    /**
     * Execute a `NOTIFY` command for the specified [channelName] with the supplied [payload]. This
     * sends a notification to any connection connected to this connection's current database.
     */
    public suspend fun notify(channelName: String, payload: String) {
        query("SELECT pg_notify($1, $2)").bind(channelName).bind(payload).execute(this)
    }

    private fun mapParameters(params: List<QueryParameter>): List<PgArgument> {
        return if (params.isEmpty()) {
            emptyList()
        } else {
            params.map { PgArgument.of(it, typeCache) }
        }
    }

    /**
     * Add new [typeDescription] to the cache. This impacts all connections within the same pool and
     * adds simple array type descriptions as well. If the [PgTypeDescription.kType] is already
     * present within the cache, that description will be removed for the new description.
     */
    public suspend fun <T : Any> registerCustomType(typeDescription: PgTypeDescription<T>) {
        typeCache.addCustomType(connection = this, typeDescription = typeDescription)
    }

    /**
     * Add a new composite type description to the type cache. Uses this connection to query the
     * database for metadata of the composite type (searching by [type]) for encoding and decoding
     * purposes. The generated [PgTypeDescription] is reflection based and has 2 checked
     * requirements:
     * 1. [T] must be a data class
     * 2. The number of parameters supplied to [T] must match the number of attributes defined for
     *    the composite type.
     *
     * The other requirements (such as the composite attribute types matching the data class) are
     * not checked at runtime so the class definer responsible for verifying them.
     *
     * This impacts all connections within the same pool and adds simple array type descriptions as
     * well. If the value class is already present within the cache, that description will be
     * removed for the new description.
     *
     * @param type name of the type in the database (optionally schema qualified if not in public
     *   schema)
     */
    public suspend inline fun <reified T : Any> registerCompositeType(
        type: String,
        compositeTypeDefinition: CompositeTypeDefinition<T>? = null,
    ) {
        val compositeTypeDescription =
            createCompositeTypeDescription(
                type = type,
                kType = typeOf<T>(),
                kClass = T::class,
                compositeTypeDefinition = compositeTypeDefinition,
            )
        this.registerCustomType(compositeTypeDescription)
    }

    /**
     * Fetch and return the type OID for a composite with the [name]. Queries the database using
     * this connection to retrieve the database instance specific OID. Returns null if the OID could
     * not be found.
     *
     * @param name Name of the composite type. Can be schema qualified but defaults to public if no
     *   schema is included
     */
    @PublishedApi
    internal suspend fun checkCompositeDbTypeByName(name: String): Int? {
        var schema: String? = null
        var typeName = name
        val schemaQualifierIndex = name.indexOf('.')
        if (schemaQualifierIndex > -1) {
            schema = name.substring(0, schemaQualifierIndex)
            typeName = name.substring(schemaQualifierIndex + 1)
        }

        val oid =
            query(pgCompositeTypeByName).bind(typeName).bind(schema).fetchScalar<Int>(this)
                ?: return null
        return oid
    }

    /**
     * Fetch and return the [PgColumnDescription]s for the composite type specified by [oid].
     * Queries the database using this connection to retrieve metadata about the composite type's
     * attributes.
     */
    @PublishedApi
    internal suspend fun getCompositeAttributeData(oid: Int): List<PgColumnDescription> {
        return query(pgCompositeTypeDetailsByOid)
            .bind(oid)
            .fetchAll(this, CompositeAttributeDataRowParser)
    }

    @PublishedApi
    internal suspend fun <T : Any> createCompositeTypeDescription(
        type: String,
        kClass: KClass<T>,
        kType: KType,
        compositeTypeDefinition: CompositeTypeDefinition<T>? = null,
    ): PgTypeDescription<T> {
        val verifiedOid =
            checkCompositeDbTypeByName(type)
                ?: throw PgException(
                    "Could not verify the composite type name '$type' in the database"
                )

        val compositeColumnMapping = getCompositeAttributeData(verifiedOid)

        val typeDef = compositeTypeDefinition ?: ReflectionCompositeTypeDescription(kClass)
        return BaseCompositeTypeDescription(
            compositeTypeDefinition = typeDef,
            typeOid = verifiedOid,
            attributeMapping = compositeColumnMapping,
            typeCache = typeCache,
            kType = kType,
        )
    }

    /**
     * Add a new enum type definition to the type cache. Uses this connection to get the enums
     * labels found in the database to compare against the supplied [enumValues]. This is the only
     * check that is required since the decoding and encoding is just reading and writing the enum
     * variants [Enum.name] value.
     *
     * This impacts all connections within the same pool and adds simple array type descriptions as
     * well. If the value class is already present within the cache, that description will be
     * removed for the new description.
     *
     * @param type name of the type in the database (optionally schema qualified if not in public
     *   schema)
     */
    public suspend inline fun <reified E : Enum<E>> registerEnumType(type: String) {
        val enumTypeDescription =
            createEnumTypeDescription(type = type, kType = typeOf<E>(), values = enumValues<E>())
        this.registerCustomType(enumTypeDescription)
    }

    /**
     * Fetch and return the type OID for an enum with the [name]. Queries the database to retrieve
     * the database instance specific OID. Returns null if the OID could not be found.
     *
     * @param name Name of the enum type. Can be schema qualified but defaults to public if no
     *   schema is included
     */
    @PublishedApi
    internal suspend fun checkEnumDbTypeByName(name: String): Int? {
        var schema: String? = null
        var typeName = name
        val schemaQualifierIndex = name.indexOf('.')
        if (schemaQualifierIndex > -1) {
            schema = name.substring(0, schemaQualifierIndex)
            typeName = name.substring(schemaQualifierIndex + 1)
        }

        val oid =
            query(pgEnumTypeByName).bind(typeName).bind(schema).fetchScalar<Int>(this)
                ?: return null
        return oid
    }

    /** [RowParser] for parsing the query result of enum type labels */
    internal object EnumLabelRowParser : RowParser<String> {
        override fun fromRow(row: DataRow): String = row.getAsNonNull("enumlabel")
    }

    /**
     * Fetch and return the labels of an enum type specified by the [oid]. Queries the database to
     * retrieve the labels.
     */
    @PublishedApi
    internal suspend fun getEnumLabels(oid: Int): List<String> {
        return query(pgEnumLabelsByOid).bind(oid).fetchAll(this, EnumLabelRowParser)
    }

    @PublishedApi
    internal suspend fun <E : Enum<E>> createEnumTypeDescription(
        type: String,
        kType: KType,
        values: Array<E>,
    ): PgTypeDescription<E> {
        val verifiedOid =
            checkEnumDbTypeByName(type)
                ?: throw PgException(
                    "Could not verify the composite type name '$type' in the database"
                )

        val enumLabels = getEnumLabels(verifiedOid)
        val enumTypeDescription =
            EnumTypeDescription(
                pgType = PgType.ByOid(oid = verifiedOid),
                kType = kType,
                values = values,
            )
        val missingLabels = enumLabels.filter { !enumTypeDescription.entryLookup.contains(it) }
        check(missingLabels.isEmpty()) {
            "Cannot register an enum type because the declared enum values do not match the " +
                "database's enum labels. Enum missing ${missingLabels.joinToString()}"
        }
        return enumTypeDescription
    }

    /**
     * Add new type description for a value class to the cache.
     *
     * This impacts all connections within the same pool and adds simple array type descriptions as
     * well. If the value class is already present within the cache, that description will be
     * removed for the new description.
     */
    public suspend inline fun <reified T : Any> registerValueType() {
        val typeDescription = createValueTypeDescription(kClass = T::class, kType = typeOf<T>())
        this.registerCustomType(typeDescription)
    }

    @PublishedApi
    internal fun <T : Any> createValueTypeDescription(
        kClass: KClass<T>,
        kType: KType,
    ): PgTypeDescription<T> {
        require(kClass.isValue) { "Type must be a value type to create wrapper type description" }
        val innerType = kClass.primaryConstructor!!.parameters.first().type
        val innerTypeDescription = typeCache.getTypeDescription<Any>(innerType)
        return ValueTypeDescription(kClass, kType, innerTypeDescription)
    }

    internal companion object {
        private const val CSV_ROW_BUFFER_SIZE = 2000
        /**
         * Magic header value required at the start a binary COPY operation
         *
         * [docs](https://www.postgresql.org/docs/current/sql-copy.html)
         */
        private val pgBinaryCopyHeader =
            byteArrayOf(
                'P'.code.toByte(),
                'G'.code.toByte(),
                'C'.code.toByte(),
                'O'.code.toByte(),
                'P'.code.toByte(),
                'Y'.code.toByte(),
                0x0A,
                -1,
                0x0D,
                0x0A,
                0x00,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
            )

        /**
         * Magic trailer value required before the end of a binary COPY operation
         *
         * [docs](https://www.postgresql.org/docs/current/sql-copy.html)
         */
        private val pgBinaryCopyTrailer = byteArrayOf(-1, -1)

        /**
         * Put the [row] data into a [ByteReadBuffer]. Has a special case where for the first row,
         * the first 19 bytes should be ignored since they are the binary copy's file header.
         */
        internal fun getBinaryBuffer(rowCount: Long, row: ByteReadBuffer): ByteReadBuffer {
            return when (rowCount) {
                1L -> {
                    row.skip(19)
                    row.slice(row.remaining)
                }
                else -> row
            }
        }

        /**
         * Query to fetch the OID of an enum using the name and the optional schema. Default schema
         * is public.
         */
        private val pgEnumTypeByName =
            """
            select t.oid
            from pg_type t
            join pg_namespace n on t.typnamespace = n.oid
            where
                t.typname = $1
                and n.nspname = coalesce(nullif($2,''), 'public')
                and t.typcategory = 'E'
            """
                .trimIndent()

        /** Query to fetch the labels of an enum type given the OID of the type */
        private val pgEnumLabelsByOid =
            """
            select e.enumlabel
            from pg_enum e
            join pg_type t on e.enumtypid = t.oid
            where
                e.enumtypid = $1
                and t.typcategory = 'E'
            """
                .trimIndent()

        /**
         * Query to fetch the OID of a composite using the name and the optional schema. Default
         * schema is public.
         */
        private val pgCompositeTypeByName =
            """
            select t.oid
            from pg_type t
            join pg_namespace n on t.typnamespace = n.oid
            where
                t.typname = $1
                and n.nspname = coalesce(nullif($2,''), 'public')
                and t.typcategory = 'C'
            """
                .trimIndent()

        /**
         * Query to fetch the attribute related data of a composite given the OID of the composite
         * type
         */
        private val pgCompositeTypeDetailsByOid =
            """
            select a.attname, a.attrelid, a.attnum, a.atttypid, a.attlen, a.atttypmod
            from pg_type t
            join pg_attribute a on t.typrelid = a.attrelid
            where
                t.oid = $1
                and t.typcategory = 'C'
                and a.attnum > 0
            """
                .trimIndent()

        /** [RowParser] for parsing the query result of composite type attributes */
        private object CompositeAttributeDataRowParser : RowParser<PgColumnDescription> {
            override fun fromRow(row: DataRow): PgColumnDescription =
                PgColumnDescription(
                    fieldName = row.getAsNonNull("attname"),
                    tableOid = row.getAsNonNull("attrelid"),
                    columnAttribute = row.getAsNonNull("attnum"),
                    pgType = PgType.fromOid(row.getAsNonNull("atttypid")),
                    dataTypeSize = row.getAsNonNull("attlen"),
                    typeModifier = row.getAsNonNull("atttypmod"),
                    formatCode = PgFormatCode.Text,
                )
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
                    throw PgException("Could not initialize connection")
                }
                return connection
            } catch (ex: Exception) {
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
