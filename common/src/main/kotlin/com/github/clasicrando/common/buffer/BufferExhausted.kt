package com.github.clasicrando.common.buffer

import com.github.clasicrando.common.exceptions.KdbcException

/** [Exception] thrown when a [ByteReadBuffer] has no more bytes to provide */
class BufferExhausted : KdbcException("Attempted to read past the buffer's size")
