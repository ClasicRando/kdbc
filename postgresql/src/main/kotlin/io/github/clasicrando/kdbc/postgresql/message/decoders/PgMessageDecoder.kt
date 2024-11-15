package io.github.clasicrando.kdbc.postgresql.message.decoders

import io.github.clasicrando.kdbc.core.message.MessageDecoder
import kotlinx.io.Source

internal abstract class PgMessageDecoder<T> : MessageDecoder<T, Unit> {
    abstract fun decode(buffer: Source): T

    final override fun decode(buffer: Source, context: Unit): T = decode(buffer)
}
