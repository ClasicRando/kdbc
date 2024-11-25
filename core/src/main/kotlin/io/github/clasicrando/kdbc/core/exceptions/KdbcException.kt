package io.github.clasicrando.kdbc.core.exceptions

import kotlin.contracts.InvocationKind
import kotlin.contracts.contract

/** Base [Exception] for all errors that occur within the realm of KDBC clients or the KDBC api */
public open class KdbcException(message: String, suppressed: Throwable? = null) :
    Exception(message, suppressed)

public inline fun checkOrKdbcException(check: Boolean, crossinline message: () -> String) {
    contract {
        returns() implies check
        callsInPlace(message, InvocationKind.AT_MOST_ONCE)
    }
    if (!check) {
        throw KdbcException(message())
    }
}
