package io.github.clasicrando.kdbc.core.stream

/** [KdbcIOException] specific to [Stream.writeTo] errors */
public class StreamWriteError(ex: Throwable) : KdbcIOException("Stream unable to write", ex)
