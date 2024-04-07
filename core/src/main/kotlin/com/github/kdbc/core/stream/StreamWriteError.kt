package com.github.kdbc.core.stream

import com.github.kdbc.core.exceptions.KdbcException

/**
 * [Exception] thrown when the write operation of an [AsyncStream] fails for whatever reason. The
 * underlining [Throwable] (if any) is suppressed.
 */
class StreamWriteError(ex: Throwable? = null) : KdbcException("Stream unable to write") {
    init {
        ex?.let { addSuppressed(it) }
    }
}
