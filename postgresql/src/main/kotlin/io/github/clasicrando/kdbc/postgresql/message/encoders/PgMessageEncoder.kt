package io.github.clasicrando.kdbc.postgresql.message.encoders

import io.github.clasicrando.kdbc.core.message.MessageEncoder
import kotlinx.io.Sink

internal abstract class PgMessageEncoder<T> : MessageEncoder<T, Unit> {
    abstract fun encode(value: T, buffer: Sink)

    final override fun encode(value: T, buffer: Sink, context: Unit) {
        encode(value, buffer)
    }
}
