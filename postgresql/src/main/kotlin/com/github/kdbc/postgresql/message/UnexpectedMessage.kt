package com.github.kdbc.postgresql.message

import com.github.kdbc.core.exceptions.KdbcException

/** [KdbcException] thrown when a [PgMessage] type was received when another was expected */
class UnexpectedMessage(reason: String) : KdbcException(reason)
