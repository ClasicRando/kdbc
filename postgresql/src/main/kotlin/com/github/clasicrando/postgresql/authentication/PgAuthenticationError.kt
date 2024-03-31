package com.github.clasicrando.postgresql.authentication

import com.github.clasicrando.common.exceptions.KdbcException

/**
 * [Exception] thrown when authentication fails against a postgres database. This can be for many
 * different reasons and the full reason should be specified in the [message] supplied.
 */
class PgAuthenticationError(message: String) : KdbcException(message)