package io.github.clasicrando.kdbc.postgresql.message

import io.github.clasicrando.kdbc.core.exceptions.KdbcException

/** [KdbcException] thrown when a [PgMessage] type was received when another was expected */
class UnexpectedMessage(reason: String) : KdbcException(reason)
