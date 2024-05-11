package io.github.clasicrando.kdbc.postgresql.result

import io.github.clasicrando.kdbc.core.Loop
import io.github.clasicrando.kdbc.core.UniqueResourceId
import io.github.clasicrando.kdbc.core.logWithResource
import io.github.clasicrando.kdbc.postgresql.GeneralPostgresError
import io.github.clasicrando.kdbc.postgresql.message.PgMessage
import io.github.clasicrando.kdbc.postgresql.message.TransactionStatus
import io.github.oshai.kotlinlogging.KotlinLogging
import io.github.oshai.kotlinlogging.Level

private val logger = KotlinLogging.logger {}

internal class CopyInResultCollector(
    private val resource: UniqueResourceId,
    private val wasFailed: Boolean
) {
    var completeMessage: PgMessage.CommandComplete? = null
    val errors = mutableListOf<Throwable>()
    var transactionStatus: TransactionStatus? = null
        private set

    fun processMessage(message: PgMessage): Loop {
        return when (message) {
            is PgMessage.ErrorResponse -> {
                if (wasFailed) {
                    completeMessage = PgMessage.CommandComplete(
                        rowCount = 0,
                        message = "CopyFail Issued by client",
                    )
                    resource.logWithResource(logger, Level.WARN) {
                        this.message = "CopyIn operation failed by client"
                    }
                    return Loop.Continue
                }
                val error = GeneralPostgresError(message)
                resource.logWithResource(logger, Level.ERROR) {
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
                transactionStatus = message.transactionStatus
                Loop.Break
            }
            else -> {
                resource.logWithResource(logger, Level.TRACE) {
                    this.message = "Ignoring {message} since it's not an error or the desired type"
                    payload = mapOf("message" to message)
                }
                Loop.Continue
            }
        }
    }
}
