package io.github.clasicrando.kdbc.postgresql.result

import io.github.clasicrando.kdbc.core.Loop
import io.github.clasicrando.kdbc.core.UniqueResourceId
import io.github.clasicrando.kdbc.core.config.Kdbc
import io.github.clasicrando.kdbc.core.logWithResource
import io.github.clasicrando.kdbc.postgresql.GeneralPostgresError
import io.github.clasicrando.kdbc.postgresql.message.PgMessage
import io.github.clasicrando.kdbc.postgresql.message.TransactionStatus
import io.github.clasicrando.kdbc.postgresql.statement.PgPreparedStatement
import io.github.oshai.kotlinlogging.KotlinLogging

private val logger = KotlinLogging.logger {}

internal class StatementPrepareRequestCollector(
    private val resource: UniqueResourceId,
    private val statement: PgPreparedStatement,
) {
    val errors = mutableListOf<Throwable>()
    var transactionStatus: TransactionStatus? = null
        private set

    fun processNextMessage(message: PgMessage): Loop {
        return when (message) {
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
                transactionStatus = message.transactionStatus
                Loop.Break
            }
            else -> logUnexpectedMessage(message)
        }
    }

    private fun logUnexpectedMessage(message: PgMessage): Loop {
        resource.logWithResource(logger, Kdbc.detailedLogging) {
            this.message = "Ignoring $message since it's not an error or the desired type"
        }
        return Loop.Continue
    }
}
