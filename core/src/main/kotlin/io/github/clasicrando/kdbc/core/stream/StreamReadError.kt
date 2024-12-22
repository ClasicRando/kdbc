package io.github.clasicrando.kdbc.core.stream

import io.github.clasicrando.kdbc.core.exceptions.KdbcException

/** [KdbcException] specific to [Stream] read errors errors */
public class StreamReadError(ex: Throwable) : KdbcException("Stream unable to read", ex)
