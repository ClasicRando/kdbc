package com.github.clasicrando.common.exceptions

class UnexpectedTransactionState(inTransaction: Boolean)
    : Throwable("Expected connection ${if (inTransaction) "" else "not "}to be transaction")
