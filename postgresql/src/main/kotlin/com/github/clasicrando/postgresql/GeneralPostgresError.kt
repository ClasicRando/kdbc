package com.github.clasicrando.postgresql

import com.github.clasicrando.postgresql.message.PgMessage
import com.github.clasicrando.postgresql.message.information.InformationResponse

class GeneralPostgresError internal constructor(
    private val errorResponse: PgMessage.ErrorResponse
) : Throwable(errorResponse.toString()) {
    val errorInformation: InformationResponse get() = errorResponse.informationResponse
}
