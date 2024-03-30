package com.github.clasicrando.common.stream

import com.github.clasicrando.common.exceptions.KdbcException

/**
 * [Exception] thrown when the write operation of an [AsyncStream] fails for whatever reason. The
 * underlining [Throwable] (if any) is suppressed.
 */
class StreamWriteError(ex: Throwable? = null) : KdbcException("Stream unable to write") {
    init {
        ex?.let { addSuppressed(it) }
    }
}
