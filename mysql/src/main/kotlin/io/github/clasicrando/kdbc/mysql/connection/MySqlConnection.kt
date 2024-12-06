package io.github.clasicrando.kdbc.mysql.connection

import io.github.clasicrando.kdbc.core.cache.LruCache
import io.github.clasicrando.kdbc.core.chunkedBytes
import io.github.clasicrando.kdbc.core.config.Kdbc
import io.github.clasicrando.kdbc.core.connection.AbstractConnection
import io.github.clasicrando.kdbc.core.connection.Connection
import io.github.clasicrando.kdbc.core.exceptions.KdbcException
import io.github.clasicrando.kdbc.core.logWithResource
import io.github.clasicrando.kdbc.core.normalizeWhitespace
import io.github.clasicrando.kdbc.core.query.Query
import io.github.clasicrando.kdbc.core.query.QueryParameter
import io.github.clasicrando.kdbc.core.query.query
import io.github.clasicrando.kdbc.core.result.DataRow
import io.github.clasicrando.kdbc.core.result.Either
import io.github.clasicrando.kdbc.core.result.QueryResult
import io.github.clasicrando.kdbc.core.splitQuery
import io.github.clasicrando.kdbc.core.statement.CsvDataRow
import io.github.clasicrando.kdbc.core.statement.mapIntoCsvByteArrayChunks
import io.github.clasicrando.kdbc.mysql.buffer.readLongLengthEncoded
import io.github.clasicrando.kdbc.mysql.exceptions.MySqlException
import io.github.clasicrando.kdbc.mysql.exceptions.checkOrMySqlException
import io.github.clasicrando.kdbc.mysql.load.LoadLocalFileStatement
import io.github.clasicrando.kdbc.mysql.message.Capabilities
import io.github.clasicrando.kdbc.mysql.message.MysqlMessage
import io.github.clasicrando.kdbc.mysql.message.Status
import io.github.clasicrando.kdbc.mysql.message.decoders.BinaryRowDecoder
import io.github.clasicrando.kdbc.mysql.message.decoders.ColumnDefinitionDecoder
import io.github.clasicrando.kdbc.mysql.message.decoders.EofDecoder
import io.github.clasicrando.kdbc.mysql.message.decoders.OkDecoder
import io.github.clasicrando.kdbc.mysql.message.decoders.PrepareOkDecoder
import io.github.clasicrando.kdbc.mysql.message.decoders.TextRowDecoder
import io.github.clasicrando.kdbc.mysql.pool.MySqlConnectionPool
import io.github.clasicrando.kdbc.mysql.result.MySqlColumn
import io.github.clasicrando.kdbc.mysql.result.MySqlDataRow
import io.github.clasicrando.kdbc.mysql.statement.MySqlArgument
import io.github.clasicrando.kdbc.mysql.statement.MySqlArguments
import io.github.clasicrando.kdbc.mysql.statement.MySqlPreparedStatement
import io.github.clasicrando.kdbc.mysql.stream.MySqlStream
import io.github.clasicrando.kdbc.mysql.stream.Waiting
import io.github.clasicrando.kdbc.mysql.type.EnumTypeDescription
import io.github.clasicrando.kdbc.mysql.type.MySqlTypeCache
import io.github.clasicrando.kdbc.mysql.type.MySqlTypeDescription
import io.github.clasicrando.kdbc.mysql.type.MysqlTypeInfo
import io.github.oshai.kotlinlogging.KLoggingEventBuilder
import io.github.oshai.kotlinlogging.KotlinLogging
import io.github.oshai.kotlinlogging.Level
import java.io.IOException
import java.io.InputStream
import java.nio.file.Files
import java.nio.file.Path
import kotlin.reflect.typeOf
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.emptyFlow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.io.Buffer
import kotlinx.io.Source
import kotlinx.io.asSource
import kotlinx.io.buffered

private val logger = KotlinLogging.logger {}

/**
 * [Connection] object for a MySQL database. A new instance cannot be created but rather the
 * [io.github.clasicrando.kdbc.mysql.MySql.connection] method should be called to receive a new
 * [MySqlConnection] ready for usage. This method will use connection pooling behind the scenes as
 * to reduce unnecessary TCP connection creation to the server when an application creates and
 * closes connections frequently.
 */
public class MySqlConnection
internal constructor(
    /** Connection options supplied when requesting a new MySQL connection */
    internal val connectionOptions: MySqlConnectionOptions,
    /** Underlining stream of data to and from the database */
    internal val stream: MySqlStream,
    /** Reference to the connection pool that owns this connection */
    internal val pool: MySqlConnectionPool,
    /** Type registry for connection. Used to decode data rows returned by the server. */
    @PublishedApi internal val typeCache: MySqlTypeCache = pool.typeCache,
) : AbstractConnection() {
    /**
     * Suspending [Mutex] to allow only 1 coroutine to execute queries against this connection. Each
     * query operation is wrapped in a [Mutex.withLock] to ensure fair but exclusive access to the
     * connection.
     */
    private val mutex = Mutex()

    override val isConnected: Boolean
        get() = stream.isConnected

    /**
     * Create a log message at the specified [level], applying the [block] to the
     * [KLogger.at][io.github.oshai.kotlinlogging.KLogger.at] method.
     */
    private inline fun log(level: Level, crossinline block: KLoggingEventBuilder.() -> Unit) {
        logWithResource(logger, level, block)
    }

    override suspend fun isValid(): Boolean {
        return mutex.withLock {
            stream.sendPacket(MysqlMessage.Ping)
            try {
                stream.receiveOk()
            } catch (_: MySqlException) {
                return false
            }
            return true
        }
    }

    /**
     * Collect all result sets sent from the server as a [Flow] of [Either] a [QueryResult] or a
     * [DataRow]. The flow will always be terminated with a [QueryResult].
     */
    private fun collectResults(isBinaryEncoding: Boolean): Flow<Either<QueryResult, DataRow>> {
        return flow {
            var columns: List<MySqlColumn>
            while (true) {
                val packet = stream.receiveNextPacket()
                val firstByte = packet.peekNextAsInt()
                if (firstByte == 0x00) {
                    val ok = OkDecoder.decode(packet)
                    emit(
                        Either.Left(
                            QueryResult(ok.affectedRows, "Last Insert ID: ${ok.lastInsertId}")
                        )
                    )

                    if (ok.status[Status.SERVER_MORE_RESULTS_EXISTS]) {
                        continue
                    }

                    stream.removeFirstWaitingIfAny()
                    return@flow
                }
                checkOrMySqlException(firstByte != 0xfb) {
                    "Found local load response. Execute that command with MySqlConnect.loadLocalFile"
                }

                stream.updateFirstWaiting(Waiting.Row)

                val columnCount = packet.readLongLengthEncoded().toInt()
                columns = receiveResultColumns(columnCount)

                var rowCount = 0L
                while (true) {
                    val rowPacket = stream.receiveNextPacket()
                    if (rowPacket.peekNextAsInt() == 0xfe && rowPacket.remaining() < 9) {
                        val eof = EofDecoder.decode(rowPacket)
                        emit(Either.Left(QueryResult(rowCount, "")))

                        if (eof.status[Status.SERVER_MORE_RESULTS_EXISTS]) {
                            stream.updateFirstWaiting(Waiting.Result)
                            break
                        }

                        stream.removeFirstWaitingIfAny()
                        return@flow
                    }

                    val rowValues =
                        if (isBinaryEncoding) {
                            BinaryRowDecoder.decode(rowPacket, columns).row
                        } else {
                            TextRowDecoder.decode(rowPacket, columns).row
                        }
                    val dataRow = MySqlDataRow(rowValues, columns, typeCache)
                    emit(Either.Right(dataRow))
                    rowCount++
                }
            }
        }
    }

    override suspend fun executeQuery(query: Query): Flow<Either<QueryResult, DataRow>> {
        val queryCount = splitQuery(query.sql).size
        if (queryCount > 1 && query.parameters.isNotEmpty()) {
            throw MySqlException(
                "Query `${query.sql}` cannot be executed as a prepared statement since it has multiple statements within"
            )
        }
        log(connectionOptions.statementLogLevel) {
            message = "Sending query: ${query.sql.normalizeWhitespace()}"
        }
        return mutex.withLock {
            stream.waitUntilReady()
            var isBinaryEncoding = false
            if (query.parameters.isNotEmpty()) {
                val statement = getOrPrepareStatement(query.sql)
                val args = MySqlArguments(query.parameters.map { MySqlArgument(it, typeCache) })
                stream.sendPacket(
                    MysqlMessage.Execute(statement = statement.statementId, arguments = args)
                )
                isBinaryEncoding = true
            } else {
                stream.sendPacket(MysqlMessage.Query(sql = query.sql))
            }
            stream.addLastWaiting(Waiting.Result)

            collectResults(isBinaryEncoding = isBinaryEncoding)
        }
    }

    override suspend fun executeQueryBatch(
        batch: List<Query>,
        withinTransaction: Boolean,
    ): Flow<Either<QueryResult, DataRow>> {
        if (batch.isEmpty()) {
            return emptyFlow()
        }
        if (batch.size == 1) {
            return executeQuery(batch[0])
        }
        if (connectionOptions.rewriteBatchInsertQuery) {
            val rewrittenQuery = attemptInsertQueriesRewrite(batch)
            if (rewrittenQuery != null) {
                return executeQuery(rewrittenQuery)
            }
        }
        return flow {
            if (withinTransaction) {
                begin()
            }
            try {
                for (query in batch) {
                    executeQuery(query).collect { emit(it) }
                }
            } catch (ex: Exception) {
                rollback()
                throw ex
            }
            if (withinTransaction) {
                commit()
            }
        }
    }

    override suspend fun close() {
        if (!pool.giveBack(this)) {
            dispose()
        }
    }

    /**
     * Issue a `LOAD DATA LOCAL` command to copy all data found when reading the [file] to the
     * server.
     *
     * A consideration for local loading is that once data has been sent to the server, it cannot be
     * aborted. This means that if you want to allow rolling back inserts where the client failed
     * part way through a read (or just generally to be safer) you should override the
     * [withTransaction] flag to true to wrap the entire operation in a transaction.
     *
     * @throws KdbcException if local loading is not supported by the server, the initial request
     *   response has an unexpected header or a general KDBC exception
     */
    public suspend fun loadLocalFile(
        statement: LoadLocalFileStatement,
        file: Path,
        withTransaction: Boolean = false,
    ): QueryResult {
        return Files.newInputStream(file).use { loadLocalFile(statement, it, withTransaction) }
    }

    /**
     * Issue a `LOAD DATA LOCAL` command to copy all [InputStream] data to the server.
     *
     * A consideration for local loading is that once data has been sent to the server, it cannot be
     * aborted. This means that if you want to allow rolling back inserts where the client failed
     * part way through a read (or just generally to be safer) you should override the
     * [withTransaction] flag to true to wrap the entire operation in a transaction.
     *
     * @throws KdbcException if local loading is not supported by the server, the initial request
     *   response has an unexpected header or a general KDBC exception
     */
    public suspend fun loadLocalFile(
        statement: LoadLocalFileStatement,
        inputStream: InputStream,
        withTransaction: Boolean = false,
    ): QueryResult {
        return loadLocalFile(statement, inputStream.asSource().buffered(), withTransaction)
    }

    /**
     * Issue a `LOAD DATA LOCAL` command to copy all [Source] data to the server.
     *
     * A consideration for local loading is that once data has been sent to the server, it cannot be
     * aborted. This means that if you want to allow rolling back inserts where the client failed
     * part way through a read (or just generally to be safer) you should override the
     * [withTransaction] flag to true to wrap the entire operation in a transaction.
     *
     * @throws KdbcException if local loading is not supported by the server, the initial request
     *   response has an unexpected header or a general KDBC exception
     */
    public suspend fun loadLocalFile(
        statement: LoadLocalFileStatement,
        source: Source,
        withTransaction: Boolean = false,
    ): QueryResult {
        return loadLocalInternal(statement, source.chunkedBytes().asFlow(), withTransaction)
    }

    /**
     * Issue a `LOAD DATA LOCAL` command to copy all [CsvDataRow]s to the server.
     *
     * A consideration for local loading is that once data has been sent to the server, it cannot be
     * aborted. This means that if you want to allow rolling back inserts where the client failed
     * part way through a read (or just generally to be safer) you should override the
     * [withTransaction] flag to true to wrap the entire operation in a transaction.
     *
     * @throws KdbcException if local loading is not supported by the server, the initial request
     *   response has an unexpected header or a general KDBC exception
     */
    public suspend fun loadLocalData(
        statement: LoadLocalFileStatement,
        data: Flow<CsvDataRow>,
        withTransaction: Boolean = false,
    ): QueryResult {
        return loadLocalInternal(
            statement =
                statement.copy(
                    characterSet = "utf8mb4",
                    delimiter = ',',
                    quoteChar = '"',
                    newline = "\n",
                    skipLines = 0,
                ),
            data = data.mapIntoCsvByteArrayChunks(CSV_ROW_BUFFER_SIZE),
            withTransaction = withTransaction,
        )
    }

    /**
     * Issue a `LOAD DATA LOCAL` command to copy all the [data] contents to the server.
     *
     * A consideration for local loading is that once data has been sent to the server, it cannot be
     * aborted. This means that if you want to allow rolling back inserts where the client failed
     * part way through a read (or just generally to be safer) you should override the
     * [withTransaction] flag to true to wrap the entire operation in a transaction.
     *
     * @throws KdbcException if local loading is not supported by the server, the initial request
     *   response has an unexpected header or a general KDBC exception
     */
    private suspend fun loadLocalInternal(
        statement: LoadLocalFileStatement,
        data: Flow<ByteArray>,
        withTransaction: Boolean = false,
    ): QueryResult {
        checkOrMySqlException(stream.capabilities[Capabilities.CLIENT_LOCAL_FILES]) {
            "Server does not support local load commands"
        }
        val query = statement.toQuery()

        if (withTransaction) {
            begin()
        }

        log(connectionOptions.statementLogLevel) {
            message = "Sending query: ${query.normalizeWhitespace()}"
        }
        try {
            stream.sendPacket(MysqlMessage.Query(sql = query))
            stream.addLastWaiting(Waiting.Result)
            val response = stream.receiveNextPacket()
            val firstByte = response.readByteAsInt()
            checkOrMySqlException(firstByte == 0xFB) {
                "LOAD LOCAL response is supposed to be 0xFB but found 0x${firstByte.toHexString()}"
            }

            var error: Exception? = null
            try {
                val tempBuffer = Buffer()
                data.collect {
                    if (tempBuffer.size + it.size > COPY_BUFFER_SIZE) {
                        stream.writePacket(MysqlMessage.LoadLocal(tempBuffer))
                    }
                    tempBuffer.write(it)
                }
                if (!tempBuffer.exhausted()) {
                    stream.writePacket(MysqlMessage.LoadLocal(tempBuffer))
                }
            } catch (ex: Exception) {
                error = ex
                throw ex
            } finally {
                if (error !is IOException) {
                    stream.writePacket(MysqlMessage.Empty)
                }
            }
            val ok = stream.receiveOk()
            stream.removeFirstWaitingIfAny()
            if (withTransaction) {
                commit()
            }
            return QueryResult(rowsAffected = ok.affectedRows, message = "LOAD DATA done")
        } catch (ex: Throwable) {
            if (withTransaction && ex !is IOException) {
                rollback()
            }
            throw ex
        }
    }

    private val lruCache =
        LruCache<String, MySqlPreparedStatement>(connectionOptions.statementCacheCapacity)

    /**
     * Pull the next N = [columnCount] packets from the server as [MysqlMessage.ColumnDefinition]
     * messages and mapping each message to a [MySqlColumn] for result set parsing.
     */
    private suspend fun receiveResultColumns(columnCount: Int): List<MySqlColumn> {
        val columns = mutableListOf<MySqlColumn>()
        for (i in 1..columnCount) {
            val columnDefinition = stream.receiveNext(ColumnDefinitionDecoder)
            val column =
                MySqlColumn(
                    ordinal = i.toLong(),
                    name = columnDefinition.columnName,
                    typeInfo = MysqlTypeInfo.fromColumnDefinition(columnDefinition),
                )
            columns.add(column)
        }

        return columns
    }

    /**
     * Send the [query] for parsing into a [MySqlPreparedStatement]. Collects all metadata sent as a
     * response to the statement prepare request.
     */
    private suspend fun prepareStatement(query: String): MySqlPreparedStatement {
        stream.sendPacket(MysqlMessage.Prepare(query))
        val prepareOk = stream.receiveNext(PrepareOkDecoder)
        if (prepareOk.params > 0) {
            (1..prepareOk.params).forEach { stream.receiveNext(ColumnDefinitionDecoder) }
        }

        val columns =
            if (prepareOk.columns > 0) {
                receiveResultColumns(prepareOk.columns)
            } else {
                emptyList()
            }

        return MySqlPreparedStatement(
            query = query,
            statementId = prepareOk.statementId,
            paramCount = prepareOk.params,
            resultMetadata = columns,
        )
    }

    /**  */
    private suspend fun getOrPrepareStatement(query: String): MySqlPreparedStatement {
        lruCache[query]?.let {
            return it
        }

        val statement = prepareStatement(query)
        lruCache.insert(query, statement)?.let {
            stream.sendPacket(MysqlMessage.StatementClose(it.value.statementId))
        }
        return statement
    }

    /**
     * Accepts a [batch] of queries and returns a single `INSERT` [Query] if all queries:
     * 1. Are the same SQL text
     * 2. Have the same number of parameters
     * 3. Match the pattern of a simple `INSERT VALUES` query
     *     - `^INSERT INTO [db.]table[(column1,....,columnN)] VALUES(?,...,?)`
     *
     * Internally the first query is used as a starting point and a `VALUES` tuple is added to the
     * final query for each batch provided. The new query string is then used to create a [Query]
     * with all parameters from each query in the batch (in order) bound to the resulting query.
     */
    private fun attemptInsertQueriesRewrite(batch: List<Query>): Query? {
        val firstSql = batch[0].sql
        val parameterCount = batch[0].parameters.size
        if (parameterCount == 0) {
            return null
        }

        val matchResult = INSERT_VALUES_REGEX.matchEntire(firstSql) ?: return null
        val parametersGroup = matchResult.groups[4]?.value ?: return null
        if (parametersGroup.count { it == '?' } != parameterCount) {
            return null
        }

        if (!batch.all { q -> q.sql == firstSql && q.parameters.size == parameterCount }) {
            return null
        }

        val values = ",(" + "?".repeat(parameterCount) + ")"
        var finalSql = StringBuilder(firstSql.trimEnd(';'))
        var parameters = mutableListOf<QueryParameter>()
        for (i in batch.indices) {
            val query = batch[i]
            if (i > 0) {
                finalSql.append(values)
            }
            parameters.addAll(query.parameters)
        }
        finalSql.append(';')
        return query(finalSql.toString()).bindMany(parameters)
    }

    /**
     * If the stream is still active send a [MysqlMessage.Quit] message before closing the
     * connection.
     */
    internal suspend fun dispose() {
        try {
            if (stream.isConnected) {
                stream.writePacket(MysqlMessage.Quit)
                log(Kdbc.detailedLogging) { this.message = "Successfully sent QUIT message" }
            }
        } catch (ex: Exception) {
            log(Level.WARN) {
                this.message = "Error sending QUIT message"
                cause = ex
            }
        } finally {
            stream.close()
        }
        lruCache.clear()
    }

    /**
     * Add new [typeDescription] to the cache. This impacts all connections within the same pool and
     * adds simple array type descriptions as well. If the [MySqlTypeDescription.kType] is already
     * present within the cache, that description will be removed for the new description.
     */
    public fun <T : Any> registerCustomType(typeDescription: MySqlTypeDescription<T>) {
        typeCache.addTypeDescription<T>(typeDescription)
    }

    /**
     * Register a new type description for an [Enum] type. The encoding and decoding is done using
     * each label's [Enum.name] property and only works for columns that are defined with an ENUM
     * constraint.
     */
    public inline fun <reified E : Enum<E>> registerEnumTypeDescription() {
        val typeDescription = EnumTypeDescription(kType = typeOf<E>(), values = enumValues<E>())
        registerCustomType<E>(typeDescription)
    }

    internal companion object {
        private const val COPY_BUFFER_SIZE = MySqlStream.MAX_PACKET_SIZE - 4
        private const val CSV_ROW_BUFFER_SIZE = 2000
        private val INSERT_VALUES_REGEX =
            Regex(
                "^INSERT\\s+INTO\\s+([0-9a-z_$]+(?:\\.[0-9a-z_$]+)?)\\(([0-9a-z_$]+(,[0-9a-z_$]+)*)\\)\\s*" +
                    "VALUES\\((\\?(?:,\\?)*)\\);?$",
                RegexOption.IGNORE_CASE,
            )

        suspend fun connect(
            connectionOptions: MySqlConnectionOptions,
            stream: MySqlStream,
            pool: MySqlConnectionPool,
        ): MySqlConnection {
            var connection: MySqlConnection? = null
            try {
                connection = MySqlConnection(connectionOptions, stream, pool)
                if (!connection.isConnected) {
                    throw MySqlException("Could not initialize connection")
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
