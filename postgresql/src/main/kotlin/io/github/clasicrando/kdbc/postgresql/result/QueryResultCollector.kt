package io.github.clasicrando.kdbc.postgresql.result

import io.github.clasicrando.kdbc.core.Loop
import io.github.clasicrando.kdbc.core.UniqueResourceId
import io.github.clasicrando.kdbc.core.logWithResource
import io.github.clasicrando.kdbc.core.result.QueryResult
import io.github.clasicrando.kdbc.core.result.StatementResult
import io.github.clasicrando.kdbc.postgresql.GeneralPostgresError
import io.github.clasicrando.kdbc.postgresql.column.PgTypeCache
import io.github.clasicrando.kdbc.postgresql.message.PgMessage
import io.github.clasicrando.kdbc.postgresql.message.TransactionStatus
import io.github.clasicrando.kdbc.postgresql.statement.PgPreparedStatement
import io.github.oshai.kotlinlogging.KotlinLogging
import io.github.oshai.kotlinlogging.Level

private val logger = KotlinLogging.logger {}

/**
 * Collector of [QueryResult]s from 1 or more [PgPreparedStatement]s and a flow of [PgMessage]s.
 *
 * [QueryResultCollector] instances are used by:
 *
 * - calling [processNextStatement] with the current [PgPreparedStatement] (or null if it's a
 * simple query)
 * - continuously calling [processNextMessage] until a [Loop.Break] result is returned
 *     - this is only returned when a receiving a ready for query message
 * - after all statements and messages have been processed, [buildStatementResult] should be called
 * to finalize the [StatementResult] building process and return the statement(s) result
 */
internal class QueryResultCollector(
    private val resource: UniqueResourceId,
    private val typeCache: PgTypeCache
) {
    /** [MutableList] of any error found while processing the backend messages */
    val errors = mutableListOf<Throwable>()
    private var currentStatement: PgPreparedStatement? = null
    private var resultSet = PgResultSet(typeCache, listOf())
    private val statementResultBuilder = StatementResult.Builder()
    /** [TransactionStatus] that should be found  */
    var transactionStatus: TransactionStatus? = null
        private set

    /**
     * Update the current [PgPreparedStatement] that is being processed. This allows for previously
     * prepared statements to provide their metadata (since no [PgMessage.RowDescription] will be
     * sent from the backend) or the collector can update the statement's metadata with the
     * received [PgMessage.RowDescription].
     */
    fun processNextStatement(statement: PgPreparedStatement?) {
        currentStatement = statement
        resultSet = PgResultSet(
            typeCache = typeCache,
            columnMapping = statement?.resultMetadata ?: emptyList(),
        )
    }

    /**
     * Process the next [message] received from the backend. Returns a [Loop] variant that
     * instructs the state machine calling this method to continue processing messages from the
     * backend or exit since the current batch of messages has been received.
     *
     * This method treats messages as follows:
     * - [PgMessage.ErrorResponse] -> packing into the [errors] collection
     * - [PgMessage.RowDescription] -> updating the [currentStatement], if any, and creating a new
     * [PgResultSet]
     * - [PgMessage.DataRow] -> pack the row into the current [resultSet]
     * - [PgMessage.CommandComplete] -> adding a new [QueryResult] to the [statementResultBuilder]
     * - [PgMessage.ReadyForQuery] -> signifies the end of the current query and tells the outer
     * state machine to exit the current loop
     */
    fun processNextMessage(message: PgMessage): Loop {
        return when (message) {
            is PgMessage.ErrorResponse -> {
                errors.add(GeneralPostgresError(message))
                Loop.Continue
            }
            is PgMessage.RowDescription -> {
                currentStatement?.resultMetadata = message.fields
                resultSet = PgResultSet(typeCache, message.fields)
                Loop.Continue
            }
            is PgMessage.DataRow -> {
                resultSet.addRow(message.rowBuffer)
                Loop.Continue
            }
            is PgMessage.CommandComplete -> {
                val queryResult = QueryResult(message.rowCount, message.message, resultSet)
                statementResultBuilder.addQueryResult(queryResult)
                Loop.Continue
            }
            is PgMessage.ReadyForQuery -> {
                transactionStatus = message.transactionStatus
                Loop.Break
            }
            else -> logUnexpectedMessage(message)
        }
    }

    /**
     * Log a [message] that is processed but ignored since it's not important during the current
     * operation
     */
    private fun logUnexpectedMessage(message: PgMessage): Loop {
        resource.logWithResource(logger, Level.TRACE) {
            this.message = "Ignoring {message} since it's not an error or the desired type"
            payload = mapOf("message" to message)
        }
        return Loop.Continue
    }

    /**
     * Finalize the [StatementResult.Builder] with all [QueryResult]s processed from the backend
     */
    fun buildStatementResult(): StatementResult {
        return statementResultBuilder.build()
    }
}
