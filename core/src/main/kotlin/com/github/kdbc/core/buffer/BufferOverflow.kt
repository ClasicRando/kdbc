package com.github.kdbc.core.buffer

import com.github.kdbc.core.exceptions.KdbcException

/**
 * [Exception] thrown when a [ByteWriteBuffer] does not have the remaining bytes available for the
 * immediate write operation.
 */
class BufferOverflow(requireSpace: Int, remaining: Int) : KdbcException(
    "Buffer does not have enough capacity (remaining = $remaining) to write $requireSpace byte(s)"
)
