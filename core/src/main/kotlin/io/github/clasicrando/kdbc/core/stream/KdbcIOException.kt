package io.github.clasicrando.kdbc.core.stream

import io.github.clasicrando.kdbc.core.exceptions.KdbcException

public open class KdbcIOException(message: String = "IO Error", cause: Throwable? = null) :
    KdbcException(message, cause)
