package io.github.clasicrando.kdbc.postgresql.statement

import io.github.clasicrando.kdbc.postgresql.buffer.writeLengthPrefixed
import io.github.clasicrando.kdbc.postgresql.type.PgTypeDescription
import kotlinx.io.Sink

/** Encode the supplied [value] in this [Sink] using the associated type description. */
internal fun <T : Any> Sink.encodeValue(value: T?, pgTypeDescription: PgTypeDescription<T>) {
    if (value == null) {
        writeInt(-1)
        return
    }
    writeLengthPrefixed { pgTypeDescription.encode(value, this) }
}
