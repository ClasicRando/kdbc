package io.github.clasicrando.kdbc.core.stream

import io.github.clasicrando.kdbc.core.exceptions.KdbcException

public class StreamWriteError(ex: Throwable) : KdbcException("Stream unable to read", ex)
