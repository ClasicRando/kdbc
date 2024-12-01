package io.github.clasicrando.kdbc.mysql.exceptions

import io.github.clasicrando.kdbc.core.exceptions.KdbcException
import kotlin.contracts.InvocationKind
import kotlin.contracts.contract

/** MySQL specific [KdbcException] */
public class MySqlException(message: String, ex: Exception? = null) : KdbcException(message, ex)

/**
 * Check the supplied boolean value and throw a [MySqlException] if the value is false. The
 * exception contains the result of the [message] lambada.
 */
public inline fun checkOrMySqlException(check: Boolean, crossinline message: () -> String) {
    contract {
        returns() implies check
        callsInPlace(message, InvocationKind.AT_MOST_ONCE)
    }
    if (!check) {
        throw MySqlException(message())
    }
}
