package io.github.clasicrando.kdbc.core.stream

import io.github.clasicrando.kdbc.core.exceptions.KdbcException

/**
 * [Exception] thrown when an [AsyncStream] is attempting to read the requested bytes but the
 * internal buffer cannot accommodate that amount of data
 */
class FullBuffer : KdbcException("AsyncStream read buffer is full but more bytes were requested")
