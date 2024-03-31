package com.github.clasicrando.common.stream

import com.github.clasicrando.common.exceptions.KdbcException

/**
 * [Exception] thrown when the read operation of an [AsyncStream] fails for whatever reason. The
 * underlining [Throwable] (if any) is suppressed.
 */
class StreamReadError(ex: Throwable? = null) : KdbcException("Stream unable to read") {
    init {
        ex?.let { addSuppressed(it) }
    }
}
