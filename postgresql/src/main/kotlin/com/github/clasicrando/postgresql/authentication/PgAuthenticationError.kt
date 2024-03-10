package com.github.clasicrando.postgresql.authentication

/**
 * [Exception] thrown when authentication fails against a postgres database. This can be for many
 * different reasons and the full reason should be specified in the [message] supplied.
 */
class PgAuthenticationError(message: String) : Exception(message)
