package com.github.kdbc.postgresql.message.information

import com.github.kdbc.core.exceptions.KdbcException

/** [Exception] thrown when parsing an [InformationResponse] but an expected field is missing */
class InvalidInformationResponse(missingField: Byte)
    : KdbcException("InformationResponse message missing expected field '$missingField'")
