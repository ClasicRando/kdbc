package io.github.clasicrando.kdbc.postgresql.exceptions

import io.github.clasicrando.kdbc.core.exceptions.KdbcException
import kotlin.contracts.InvocationKind
import kotlin.contracts.contract

/** Postgresql specific [KdbcException] */
public open class PgException(message: String, ex: Exception? = null) : KdbcException(message, ex)

/**
 * Check the supplied boolean value and throw a [PgException] if the value is false. The exception
 * contains the result of the [message] lambada.
 */
public inline fun checkOrPgException(check: Boolean, crossinline message: () -> String) {
    contract {
        returns() implies check
        callsInPlace(message, InvocationKind.AT_MOST_ONCE)
    }
    if (!check) {
        throw PgException(message())
    }
}
