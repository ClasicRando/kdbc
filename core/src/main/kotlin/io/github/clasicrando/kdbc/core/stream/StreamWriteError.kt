package io.github.clasicrando.kdbc.core.stream

import io.github.clasicrando.kdbc.core.exceptions.KdbcException

/** [KdbcException] specific to [Stream.writeTo] errors */
public class StreamWriteError(ex: Throwable) : KdbcException("Stream unable to write", ex)
