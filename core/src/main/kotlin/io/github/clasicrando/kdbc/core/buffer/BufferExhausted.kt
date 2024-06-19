package io.github.clasicrando.kdbc.core.buffer

import io.github.clasicrando.kdbc.core.exceptions.KdbcException

/** [Exception] thrown when a [ByteReadBuffer] has no more bytes to provide */
class BufferExhausted(requested: Int, remaining: Int) : KdbcException(
    "Attempted to read past the buffer's size. Requested: $requested bytes, Remaining: $remaining bytes"
)
