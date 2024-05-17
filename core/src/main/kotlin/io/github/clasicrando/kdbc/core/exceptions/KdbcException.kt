package io.github.clasicrando.kdbc.core.exceptions

/** Base [Exception] for all errors that occur within the realm of KDBC clients or the KDBC api */
open class KdbcException(message: String, suppressed: Throwable? = null)
    : Exception(message, suppressed)
