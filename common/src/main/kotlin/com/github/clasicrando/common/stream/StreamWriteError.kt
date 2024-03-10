package com.github.clasicrando.common.stream

/**
 * [Exception] thrown when the write operation of an [AsyncStream] fails for whatever reason. The
 * underlining [Throwable] (if any) is suppressed.
 */
class StreamWriteError(ex: Throwable? = null) : Exception() {
    init {
        ex?.let { addSuppressed(it) }
    }
}
