package io.github.clasicrando.kdbc.core.exceptions

import io.github.clasicrando.kdbc.core.connection.Connection

/**
 * Exception thrown when the transaction status of a [Connection] is the opposite assumed by the
 * current operation.
 *
 * *Example*
 * After successfully beginning a new transaction against a [Connection], the previous transaction
 * state should have been `inTransaction = false`. If that property was true, this exception should
 * be thrown to signify the [Connection] is in an invalid/unknown state and the underlining
 * connection should be terminated.
 */
class UnexpectedTransactionState(inTransaction: Boolean)
    : KdbcException("Expected connection ${if (inTransaction) "" else "not "}to be transaction")
