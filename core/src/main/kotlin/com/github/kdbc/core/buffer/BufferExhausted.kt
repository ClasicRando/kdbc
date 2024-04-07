package com.github.kdbc.core.buffer

import com.github.kdbc.core.exceptions.KdbcException

/** [Exception] thrown when a [ByteReadBuffer] has no more bytes to provide */
class BufferExhausted : KdbcException("Attempted to read past the buffer's size")
