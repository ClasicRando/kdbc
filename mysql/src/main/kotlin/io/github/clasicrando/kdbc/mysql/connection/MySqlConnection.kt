package io.github.clasicrando.kdbc.mysql.connection

import io.github.clasicrando.kdbc.core.cache.LruCache
import io.github.clasicrando.kdbc.core.config.Kdbc
import io.github.clasicrando.kdbc.core.connection.AbstractConnection
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
import io.github.clasicrando.kdbc.mysql.buffer.readByteAsInt
import io.github.clasicrando.kdbc.mysql.buffer.readLongLengthEncoded
import io.github.clasicrando.kdbc.mysql.exceptions.MySqlException
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
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.emptyFlow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlin.reflect.typeOf

private val logger = KotlinLogging.logger {}

public class MySqlConnection
internal constructor(
    internal val connectionOptions: MySqlConnectionOptions,
    internal val stream: MySqlStream,
    internal val pool: MySqlConnectionPool,
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

    private fun collectResults(
        needsMetadataParam: Boolean,
        isBinaryEncoding: Boolean,
    ): Flow<Either<QueryResult, DataRow>> = flow {
        var columns: List<MySqlColumn>
        var needsMetadata = needsMetadataParam
        while (true) {
            val packet = stream.receiveNextPacket()
            val firstByte = packet.peek().readByteAsInt()
            if (firstByte == 0x00) {
                val ok = OkDecoder.decode(packet)
                emit(
                    Either.Left(QueryResult(ok.affectedRows, "Last Insert ID: ${ok.lastInsertId}"))
                )

                if (ok.status[Status.SERVER_MORE_RESULTS_EXISTS]) {
                    continue
                }

                stream.removeFirstWaiting()
                return@flow
            }

            stream.updateFirstWaiting(Waiting.Row)

            val columnCount = packet.readLongLengthEncoded().toInt()
            if (needsMetadata) {
                columns = receiveResultColumns(columnCount, true)
            } else {
                needsMetadata = true
                columns = receiveResultColumns(columnCount, false)
            }

            var rowCount = 0L
            while (true) {
                val rowPacket = stream.receiveNextPacket()
                if (rowPacket.peek().readByteAsInt() == 0xfe && rowPacket.size < 9) {
                    val eof = EofDecoder.decode(rowPacket)
                    emit(Either.Left(QueryResult(rowCount, "")))

                    if (eof.status[Status.SERVER_MORE_RESULTS_EXISTS]) {
                        stream.updateFirstWaiting(Waiting.Result)
                        break
                    }

                    stream.removeFirstWaiting()
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
            stream.addLastWaiting(Waiting.Result)
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

            collectResults(
                needsMetadataParam = query.parameters.isEmpty(),
                isBinaryEncoding = isBinaryEncoding,
            )
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

    private val lruCache =
        LruCache<String, MySqlPreparedStatement>(connectionOptions.statementCacheCapacity)

    private suspend fun receiveResultColumns(
        columnCount: Int,
        alwaysCheckEof: Boolean,
    ): List<MySqlColumn> {
        val columns = mutableListOf<MySqlColumn>()
        for (i in 1..columnCount) {
            val columnDefinition = stream.receiveNext(ColumnDefinitionDecoder)
            val column =
                MySqlColumn(
                    ordinal = i.toLong(),
                    name = columnDefinition.getName(),
                    typeInfo = MysqlTypeInfo.fromColumnDefinition(columnDefinition),
                )
            columns.add(column)
        }

        if (alwaysCheckEof || columnCount > 0) {
            stream.receiveEofIfPossible()
        }

        return columns
    }

    private suspend fun receiveResultMetadata(
        columnCount: Int
    ): Pair<Map<String, Int>, List<MySqlColumn>> {
        val columnNames = mutableMapOf<String, Int>()
        val columns = mutableListOf<MySqlColumn>()
        for (i in 1..columnCount) {
            val columnDefinition = stream.receiveNext(ColumnDefinitionDecoder)
            val column =
                MySqlColumn(
                    ordinal = i.toLong(),
                    name = columnDefinition.getName(),
                    typeInfo = MysqlTypeInfo.fromColumnDefinition(columnDefinition),
                )

            columnNames[column.name] = i
            columns.add(column)
        }
        stream.receiveEofIfPossible()
        return columnNames to columns
    }

    private suspend fun prepareStatement(query: String): MySqlPreparedStatement {
        stream.sendPacket(MysqlMessage.Prepare(query))
        val prepareOk = stream.receiveNext(PrepareOkDecoder)
        if (prepareOk.params > 0) {
            (1..prepareOk.params).forEach { stream.receiveNext(ColumnDefinitionDecoder) }
            stream.receiveEofIfPossible()
        }

        val (columnNames, columns) =
            if (prepareOk.columns > 0) {
                receiveResultMetadata(prepareOk.columns)
            } else {
                emptyMap<String, Int>() to emptyList()
            }

        return MySqlPreparedStatement(
            query = query,
            statementId = prepareOk.statementId,
            paramCount = prepareOk.params,
            columns = columns,
            columnNames = columnNames,
        )
    }

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

    private fun attemptInsertQueriesRewrite(batch: List<Query>): Query? {
        val firstSql = batch[0].sql
        val parameterCount = batch[0].parameters.size
        if (!firstSql.startsWith("INSERT", ignoreCase = true) || parameterCount == 0) {
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

    public fun <T : Any> registerCustomType(typeDescription: MySqlTypeDescription<T>) {
        typeCache.addTypeDescription<T>(typeDescription)
    }

    public inline fun <reified E : Enum<E>> addEnumTypeDescription() {
        val typeDescription = EnumTypeDescription(kType = typeOf<E>(), values = enumValues<E>())
        registerCustomType<E>(typeDescription)
    }

    internal companion object {
        suspend fun connect(
            connectionOptions: MySqlConnectionOptions,
            stream: MySqlStream,
            pool: MySqlConnectionPool,
        ): MySqlConnection {
            var connection: MySqlConnection? = null
            try {
                connection = MySqlConnection(connectionOptions, stream, pool)
                if (!connection.isConnected) {
                    throw KdbcException("Could not initialize connection")
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
