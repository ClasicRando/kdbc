package io.github.clasicrando.kdbc.postgresql.copy

import io.github.clasicrando.kdbc.core.exceptions.KdbcException
import io.github.clasicrando.kdbc.postgresql.buffer.writeLengthPrefixed
import io.github.clasicrando.kdbc.postgresql.type.PgTypeCache
import kotlin.reflect.KType
import kotlin.reflect.typeOf
import kotlinx.io.Buffer

public class PgCopyEncodeBuffer internal constructor(private val typeCache: PgTypeCache) :
    AutoCloseable {
    internal val innerBuffer = Buffer()

    private fun <T : Any> encodeNonNullValue(value: T, kType: KType) {
        val description =
            typeCache.getTypeDescription<T>(kType)
                ?: throw KdbcException("Could not find type description for $kType")
        innerBuffer.writeLengthPrefixed { description.encode(value, this) }
    }

    public fun <T : Any> encodeValue(value: T?, kType: KType) {
        if (value == null) {
            innerBuffer.writeInt(-1)
            return
        }
        encodeNonNullValue(value, kType)
    }

    public inline fun <reified T : Any> encodeValue(value: T?) {
        encodeValue(value, typeOf<T>())
    }

    override fun close() {
        innerBuffer.close()
    }
}
