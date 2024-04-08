package io.github.clasicrando.kdbc.postgresql.stream

import io.github.clasicrando.kdbc.core.buffer.ByteReadBuffer

/**
 * General wrapper for a postgresql backend message. Contents are laid out as:
 * - a header [format] [Byte]
 * - a [size] [UInt] value (tells the client how many more byte are part of the message)
 * - the [contents] of the message as a [ByteReadBuffer] (size of the buffer corresponds to the
 * [size] value)
 */
internal data class RawMessage(val format: Byte, val size: UInt, val contents: ByteReadBuffer)
