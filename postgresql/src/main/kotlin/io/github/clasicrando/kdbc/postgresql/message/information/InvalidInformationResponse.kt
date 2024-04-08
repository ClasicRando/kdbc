package io.github.clasicrando.kdbc.postgresql.message.information

import io.github.clasicrando.kdbc.core.exceptions.KdbcException

/** [Exception] thrown when parsing an [InformationResponse] but an expected field is missing */
class InvalidInformationResponse(missingField: Byte)
    : KdbcException("InformationResponse message missing expected field '$missingField'")
