package io.github.clasicrando.kdbc.postgresql.message.encoders

import io.github.clasicrando.kdbc.core.buffer.ByteWriteBuffer
import io.github.clasicrando.kdbc.core.message.MessageEncoder

internal abstract class PgMessageEncoder<T> : MessageEncoder<T, Unit> {
    abstract fun encode(value: T, buffer: ByteWriteBuffer)

    final override fun encode(value: T, buffer: ByteWriteBuffer, context: Unit) {
        encode(value, buffer)
    }
}
