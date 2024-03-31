package com.github.clasicrando.postgresql

import com.github.clasicrando.common.exceptions.KdbcException
import com.github.clasicrando.postgresql.message.PgMessage
import com.github.clasicrando.postgresql.message.information.InformationResponse

/** [KdbcException] thrown when the postgresql server returns a [PgMessage.ErrorResponse] */
class GeneralPostgresError internal constructor(
    private val errorResponse: PgMessage.ErrorResponse
) : KdbcException("General Postgresql Error:\n" + errorResponse.informationResponse.toString()) {
    val errorInformation: InformationResponse get() = errorResponse.informationResponse
}
