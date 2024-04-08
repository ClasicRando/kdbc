package io.github.clasicrando.kdbc.core.buffer

import io.github.clasicrando.kdbc.core.exceptions.KdbcException

/**
 * [Exception] thrown when a [ByteWriteBuffer] does not have the remaining bytes available for the
 * immediate write operation.
 */
class BufferOverflow(requireSpace: Int, remaining: Int) : KdbcException(
    "Buffer does not have enough capacity (remaining = $remaining) to write $requireSpace byte(s)"
)
