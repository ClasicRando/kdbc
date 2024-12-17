package io.github.clasicrando.kdbc.core.stream

import io.github.clasicrando.kdbc.core.exceptions.KdbcException

public class StreamReadError(ex: Throwable) : KdbcException("Stream unable to write", ex)
