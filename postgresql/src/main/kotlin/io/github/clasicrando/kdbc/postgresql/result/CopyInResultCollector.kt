package io.github.clasicrando.kdbc.postgresql.result

import io.github.clasicrando.kdbc.core.Loop
import io.github.clasicrando.kdbc.core.UniqueResourceId
import io.github.clasicrando.kdbc.core.config.Kdbc
import io.github.clasicrando.kdbc.core.logWithResource
import io.github.clasicrando.kdbc.postgresql.GeneralPostgresError
import io.github.clasicrando.kdbc.postgresql.message.PgMessage
import io.github.clasicrando.kdbc.postgresql.message.TransactionStatus
import io.github.oshai.kotlinlogging.KotlinLogging
import io.github.oshai.kotlinlogging.Level

private val logger = KotlinLogging.logger {}

/**
 * Collector of post `COPY IN` backend messages.
 *
 * [CopyInResultCollector] instances are used by calling [processMessage] message continuously
 * until it returns [Loop.Break] (i.e. a [PgMessage.ReadyForQuery] message is sent from the
 * backend).
 */
internal class CopyInResultCollector(
    private val resource: UniqueResourceId,
    private val wasFailed: Boolean,
) {
    /**
     * Query completion message that was found while collecting result messages. Null means the
     * query result is still being processed
     */
    var completeMessage: PgMessage.CommandComplete? = null
        private set

    /** [MutableList] of any error found while processing the backend messages */
    val errors = mutableListOf<Throwable>()

    /**
     * The current connection's [TransactionStatus] after completing the `COPY IN` query. Null
     * means the query is still being processed.
     */
    var transactionStatus: TransactionStatus? = null
        private set

    /**
     * Process the next [message] received from the backend. Returns a [Loop] variant that
     * instructs the state machine calling this method to continue processing messages from the
     * backend or exit since the current batch of messages has been received.
     *
     * This method treats messages as follows:
     * - [PgMessage.ErrorResponse] -> packing into the [errors] collection if the `COPY IN`
     * was not failed by the client (i.e. [wasFailed] is false)
     *     - If [wasFailed] is true, the [completeMessage] is set with appropriate details
     * - [PgMessage.CommandComplete] -> set [completeMessage] with the current message
     * - [PgMessage.ReadyForQuery] -> signifies the end of the current query and tells the outer
     * state machine to exit the current loop
     */
    fun processMessage(message: PgMessage): Loop {
        return when (message) {
            is PgMessage.ErrorResponse -> {
                if (wasFailed) {
                    completeMessage =
                        PgMessage.CommandComplete(
                            rowCount = 0,
                            message = "CopyFail Issued by client",
                        )
                    resource.logWithResource(logger, Level.WARN) {
                        this.message = "CopyIn operation failed by client"
                    }
                    return Loop.Continue
                }
                val error = GeneralPostgresError(message)
                resource.logWithResource(logger, Kdbc.detailedLogging) {
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
                resource.logWithResource(logger, Kdbc.detailedLogging) {
                    this.message = "Ignoring $message since it's not an error or the desired type"
                }
                Loop.Continue
            }
        }
    }
}
