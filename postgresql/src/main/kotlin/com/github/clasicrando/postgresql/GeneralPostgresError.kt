package com.github.clasicrando.postgresql

import com.github.clasicrando.postgresql.message.PgMessage

internal class GeneralPostgresError(errorResponse: PgMessage.ErrorResponse)
    : Throwable(errorResponse.fields.entries.joinToString { "${it.key} -> ${it.value}" })
