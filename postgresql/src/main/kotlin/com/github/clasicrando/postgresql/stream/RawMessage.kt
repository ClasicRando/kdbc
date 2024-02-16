package com.github.clasicrando.postgresql.stream

import com.github.clasicrando.common.buffer.ReadBuffer

internal data class RawMessage(val format: Byte, val size: UInt, val contents: ReadBuffer)
