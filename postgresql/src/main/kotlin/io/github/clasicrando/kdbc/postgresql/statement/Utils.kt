package io.github.clasicrando.kdbc.postgresql.statement

import io.github.clasicrando.kdbc.core.exceptions.KdbcException
import io.github.clasicrando.kdbc.postgresql.buffer.writeLengthPrefixedInt
import io.github.clasicrando.kdbc.postgresql.type.PgTypeCache
import kotlin.reflect.KType
import kotlinx.io.Sink

internal fun Sink.encodeValue(value: Any?, type: KType, typeCache: PgTypeCache) {
    if (value == null) {
        writeInt(-1)
        return
    }
    val description =
        typeCache.getTypeDescription<Any>(type)
            ?: throw KdbcException("Could not find type description for $type")
    writeLengthPrefixedInt { description.encode(value, this) }
}
