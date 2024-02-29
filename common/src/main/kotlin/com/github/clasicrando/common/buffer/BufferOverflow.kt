package com.github.clasicrando.common.buffer

/**
 * [Exception] thrown when a [ByteWriteBuffer] does not have the remaining bytes available for the
 * immediate write operation.
 */
class BufferOverflow(requireSpace: Int, remaining: Int) : Exception(
    "Buffer does not have enough capacity (remaining = $remaining) to write $requireSpace byte(s)"
)
