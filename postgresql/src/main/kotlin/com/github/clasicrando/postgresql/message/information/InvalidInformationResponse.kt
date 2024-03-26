package com.github.clasicrando.postgresql.message.information

/** [Exception] thrown when parsing an [InformationResponse] but an expected field is missing */
class InvalidInformationResponse(missingField: Byte)
    : Exception("InformationResponse message missing expected field '$missingField'")
