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

internal class QueryResultCollector(
    private val resource: UniqueResourceId,
    private val typeCache: PgTypeCache
) {
    val errors = mutableListOf<Throwable>()
    private var currentStatement: PgPreparedStatement? = null
    var result = PgResultSet(typeCache, listOf())
        private set
    private val statementResultBuilder = StatementResult.Builder()
    var transactionStatus: TransactionStatus? = null
        private set

    fun processNextStatement(statement: PgPreparedStatement?) {
        currentStatement = statement
        result = PgResultSet(
            typeCache = typeCache,
            columnMapping = statement?.resultMetadata ?: emptyList(),
        )
    }

    fun processNextMessage(message: PgMessage): Loop {
        return when (message) {
            is PgMessage.ErrorResponse -> {
                errors.add(GeneralPostgresError(message))
                Loop.Continue
            }
            is PgMessage.RowDescription -> {
                currentStatement?.resultMetadata = message.fields
                result = PgResultSet(typeCache, message.fields)
                Loop.Continue
            }
            is PgMessage.DataRow -> {
                result.addRow(message.rowBuffer)
                Loop.Continue
            }
            is PgMessage.CommandComplete -> {
                val queryResult = QueryResult(message.rowCount, message.message, result)
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

    private fun logUnexpectedMessage(message: PgMessage): Loop {
        resource.logWithResource(logger, Level.TRACE) {
            this.message = "Ignoring {message} since it's not an error or the desired type"
            payload = mapOf("message" to message)
        }
        return Loop.Continue
    }

    fun buildStatementResult(): StatementResult {
        return statementResultBuilder.build()
    }
}
