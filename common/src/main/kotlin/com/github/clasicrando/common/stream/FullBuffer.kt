package com.github.clasicrando.common.stream

import com.github.clasicrando.common.exceptions.KdbcException

/**
 * [Exception] thrown when an [AsyncStream] is attempting to read the requested bytes but the
 * internal buffer cannot accommodate that amount of data
 */
class FullBuffer : KdbcException("AsyncStream read buffer is full but more bytes were requested")
