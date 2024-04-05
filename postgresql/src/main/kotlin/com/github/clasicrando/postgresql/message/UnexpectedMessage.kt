package com.github.clasicrando.postgresql.message

import com.github.clasicrando.common.exceptions.KdbcException

/** [KdbcException] thrown when a [PgMessage] type was received when another was expected */
class UnexpectedMessage(reason: String) : KdbcException(reason)
