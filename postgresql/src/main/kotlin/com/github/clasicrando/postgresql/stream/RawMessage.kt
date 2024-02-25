package com.github.clasicrando.postgresql.stream

import com.github.clasicrando.common.buffer.ByteReadBuffer

internal data class RawMessage(val format: Byte, val size: UInt, val contents: ByteReadBuffer)
