package io.github.clasicrando.kdbc.postgresql.copy

import io.github.clasicrando.kdbc.postgresql.statement.encodeValue
import io.github.clasicrando.kdbc.postgresql.type.PgTypeCache
import kotlin.reflect.KType
import kotlin.reflect.typeOf
import kotlinx.io.Buffer

public class PgCopyEncodeBuffer internal constructor(private val typeCache: PgTypeCache) :
    AutoCloseable {
    internal val innerBuffer = Buffer()

    public fun <T : Any> encodeValue(value: T?, kType: KType) {
        innerBuffer.encodeValue(value, kType, typeCache)
    }

    public inline fun <reified T : Any> encodeValue(value: T?) {
        encodeValue(value, typeOf<T>())
    }

    override fun close() {
        innerBuffer.close()
    }
}
