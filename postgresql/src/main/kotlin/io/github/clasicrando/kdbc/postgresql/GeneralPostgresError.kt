package io.github.clasicrando.kdbc.postgresql

import io.github.clasicrando.kdbc.core.exceptions.KdbcException
import io.github.clasicrando.kdbc.postgresql.message.PgMessage
import io.github.clasicrando.kdbc.postgresql.message.information.InformationResponse

/** [KdbcException] thrown when the postgresql server returns a [PgMessage.ErrorResponse] */
class GeneralPostgresError internal constructor(
    private val errorResponse: PgMessage.ErrorResponse
) : KdbcException("General Postgresql Error:\n" + errorResponse.informationResponse.toString()) {
    val errorInformation: InformationResponse get() = errorResponse.informationResponse
}
