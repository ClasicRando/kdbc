package io.github.clasicrando.kdbc.core.stream

/** [KdbcIOException] specific to [Stream] read errors errors */
public class StreamReadError(ex: Throwable) : KdbcIOException("Stream unable to read", ex)
