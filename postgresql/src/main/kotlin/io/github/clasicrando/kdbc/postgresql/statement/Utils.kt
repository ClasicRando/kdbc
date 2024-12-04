package io.github.clasicrando.kdbc.postgresql.statement

import io.github.clasicrando.kdbc.core.exceptions.KdbcException
import io.github.clasicrando.kdbc.core.query.QueryParameter
import io.github.clasicrando.kdbc.postgresql.buffer.writeLengthPrefixed
import io.github.clasicrando.kdbc.postgresql.type.PgTypeCache
import io.github.clasicrando.kdbc.postgresql.type.PgTypeDescription
import kotlin.reflect.KType
import kotlinx.io.Sink

/**
 * Encode the supplied [queryParameter] in this [Sink], looking up the type definition in the
 * [typeCache]
 */
internal fun Sink.encodeValue(queryParameter: QueryParameter, typeCache: PgTypeCache) {
    encodeValue(queryParameter.value, queryParameter.parameterType, typeCache)
}

/**
 * Encode the supplied [value] of type [T] in this [Sink], looking up the type definition in the
 * [typeCache]
 */
internal fun <T : Any> Sink.encodeValue(value: T?, type: KType, typeCache: PgTypeCache) {
    val description =
        typeCache.getTypeDescription<Any>(type)
            ?: throw KdbcException("Could not find type description for $type")
    encodeValue(value, description)
}

/** Encode the supplied [value] in this [Sink] using the associated type description. */
internal fun <T : Any> Sink.encodeValue(value: T?, pgTypeDescription: PgTypeDescription<T>) {
    if (value == null) {
        writeInt(-1)
        return
    }
    writeLengthPrefixed { pgTypeDescription.encode(value, this) }
}
