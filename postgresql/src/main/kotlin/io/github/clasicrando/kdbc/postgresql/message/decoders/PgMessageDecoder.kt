package io.github.clasicrando.kdbc.postgresql.message.decoders

import io.github.clasicrando.kdbc.core.message.MessageDecoder
import kotlinx.io.Buffer

internal abstract class PgMessageDecoder<T> : MessageDecoder<T, Unit> {
    abstract fun decode(buffer: Buffer): T

    final override fun decode(buffer: Buffer, context: Unit): T = decode(buffer)
}
