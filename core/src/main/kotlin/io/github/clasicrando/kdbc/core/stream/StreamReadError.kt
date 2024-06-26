package io.github.clasicrando.kdbc.core.stream

import io.github.clasicrando.kdbc.core.exceptions.KdbcException

/**
 * [Exception] thrown when the read operation of an [AsyncStream] fails for whatever reason. The
 * underlining [Throwable] (if any) is suppressed.
 */
class StreamReadError(ex: Throwable? = null) : KdbcException("Stream unable to read") {
    init {
        ex?.let { addSuppressed(it) }
    }
}
