package com.github.clasicrando.common.exceptions

/**
 * Exception thrown when the transaction status of a
 * [Connection][com.github.clasicrando.common.connection.Connection] is the opposite assumed by the
 * current operation.
 *
 * *Example*
 * After successfully beginning a new transaction against a
 * [Connection][com.github.clasicrando.common.connection.Connection], the previous transaction
 * state should have been `inTransaction = false`. If that property was true, this exception should
 * be thrown to signify the [Connection][com.github.clasicrando.common.connection.Connection] is
 * in an invalid/unknown state and the underlining connection should be terminated.
 */
class UnexpectedTransactionState(inTransaction: Boolean)
    : KdbcException("Expected connection ${if (inTransaction) "" else "not "}to be transaction")
