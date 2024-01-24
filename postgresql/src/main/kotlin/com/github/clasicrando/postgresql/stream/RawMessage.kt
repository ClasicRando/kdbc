package com.github.clasicrando.postgresql.stream

import io.ktor.utils.io.core.ByteReadPacket

internal data class RawMessage(val format: Byte, val size: UInt, val contents: ByteReadPacket)
