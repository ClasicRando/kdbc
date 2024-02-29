package com.github.clasicrando.common.buffer

/** [Exception] thrown when a [ByteReadBuffer] has no more bytes to provide */
class BufferExhausted : Exception("Attempted to read past the buffer's size")
