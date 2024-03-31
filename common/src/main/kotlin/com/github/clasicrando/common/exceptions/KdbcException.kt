package com.github.clasicrando.common.exceptions

/** Base [Exception] for all errors that occur within the realm of KDBC clients or the KDBC api */
open class KdbcException(message: String) : Exception(message)
