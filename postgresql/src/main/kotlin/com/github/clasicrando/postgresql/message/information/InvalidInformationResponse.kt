package com.github.clasicrando.postgresql.message.information

import com.github.clasicrando.common.exceptions.KdbcException

/** [Exception] thrown when parsing an [InformationResponse] but an expected field is missing */
class InvalidInformationResponse(missingField: Byte)
    : KdbcException("InformationResponse message missing expected field '$missingField'")
