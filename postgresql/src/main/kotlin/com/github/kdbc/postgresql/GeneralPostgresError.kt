package com.github.kdbc.postgresql

import com.github.kdbc.core.exceptions.KdbcException
import com.github.kdbc.postgresql.message.PgMessage
import com.github.kdbc.postgresql.message.information.InformationResponse

/** [KdbcException] thrown when the postgresql server returns a [PgMessage.ErrorResponse] */
class GeneralPostgresError internal constructor(
    private val errorResponse: PgMessage.ErrorResponse
) : KdbcException("General Postgresql Error:\n" + errorResponse.informationResponse.toString()) {
    val errorInformation: InformationResponse get() = errorResponse.informationResponse
}
