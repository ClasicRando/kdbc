package io.github.clasicrando.kdbc.postgresql.stream

import kotlinx.io.Buffer

/**
 * General wrapper for a postgresql backend message. Contents are laid out as:
 * - a header [format] [Byte]
 * - a [size] [Int] value (tells the client how many more byte are part of the message)
 * - the [contents] of the message as a [Buffer] (size of the buffer corresponds to the [size]
 *   value)
 */
internal data class RawMessage(val format: Byte, val size: Int, val contents: Buffer)
