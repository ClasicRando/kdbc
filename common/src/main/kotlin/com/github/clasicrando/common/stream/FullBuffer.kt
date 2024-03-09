package com.github.clasicrando.common.stream

/**
 * [Exception] thrown when an [AsyncStream] is attempting to read the requested bytes but the
 * internal buffer cannot accommodate that amount of data
 */
class FullBuffer : Exception("AsyncStream read buffer is full but more bytes were requested")
