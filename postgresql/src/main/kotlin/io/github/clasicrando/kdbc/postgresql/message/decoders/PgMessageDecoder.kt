package io.github.clasicrando.kdbc.postgresql.message.decoders

import io.github.clasicrando.kdbc.core.buffer.ByteReadBuffer
import io.github.clasicrando.kdbc.core.message.MessageDecoder

internal abstract class PgMessageDecoder<T> : MessageDecoder<T, Unit> {
    abstract fun decode(buffer: ByteReadBuffer): T

    final override fun decode(buffer: ByteReadBuffer, context: Unit): T = decode(buffer)
}
