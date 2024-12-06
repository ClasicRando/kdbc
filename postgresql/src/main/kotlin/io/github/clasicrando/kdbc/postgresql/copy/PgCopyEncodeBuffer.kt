package io.github.clasicrando.kdbc.postgresql.copy

import io.github.clasicrando.kdbc.postgresql.statement.encodeValue
import io.github.clasicrando.kdbc.postgresql.type.PgTypeCache
import kotlinx.io.Buffer
import kotlin.reflect.KType
import kotlin.reflect.typeOf

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
