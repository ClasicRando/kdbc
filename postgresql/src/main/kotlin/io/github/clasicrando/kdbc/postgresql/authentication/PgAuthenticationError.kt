package io.github.clasicrando.kdbc.postgresql.authentication

import io.github.clasicrando.kdbc.core.exceptions.KdbcException

/**
 * [Exception] thrown when authentication fails against a postgres database. This can be for many
 * different reasons and the full reason should be specified in the [message] supplied.
 */
public class PgAuthenticationError(message: String) : KdbcException(message)
