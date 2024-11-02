package io.github.clasicrando.kdbc.postgresql.statement

import io.github.clasicrando.kdbc.core.buffer.ByteWriteBuffer
import io.github.clasicrando.kdbc.core.buffer.writeLengthPrefixed
import io.github.clasicrando.kdbc.core.exceptions.KdbcException
import io.github.clasicrando.kdbc.postgresql.type.PgTypeCache
import io.github.clasicrando.kdbc.postgresql.type.PgTypeDescription
import kotlin.reflect.KType

internal fun ByteWriteBuffer.encodeValue(
    value: Any?,
    type: KType,
    typeCache: PgTypeCache,
) {
    if (value == null) {
        writeInt(-1)
        return
    }
    val description =
        typeCache.getTypeDescription<Any>(type)
            ?: throw KdbcException("Could not find type description for $type")
    writeLengthPrefixed {
        description.encode(value, this)
    }
}

internal fun ByteWriteBuffer.encodeValue(
    value: Any?,
    typeDescription: PgTypeDescription<Any>,
) {
    if (value == null) {
        writeInt(-1)
        return
    }
    writeLengthPrefixed {
        typeDescription.encode(value, this)
    }
}
