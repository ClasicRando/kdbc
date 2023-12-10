package com.github.clasicrando.common.exceptions

/**
 * Exception thrown when the status of a
 * [Connection][com.github.clasicrando.common.connection.Connection] is the opposite assumed by the
 * current operation.
 */
class UnexpectedTransactionState(inTransaction: Boolean)
    : Throwable("Expected connection ${if (inTransaction) "" else "not "}to be transaction")
