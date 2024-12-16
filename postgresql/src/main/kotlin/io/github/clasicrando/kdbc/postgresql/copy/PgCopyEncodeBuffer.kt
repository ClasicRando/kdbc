package io.github.clasicrando.kdbc.postgresql.copy

import io.github.clasicrando.kdbc.core.buffer.ByteWriteBuffer
import io.github.clasicrando.kdbc.postgresql.statement.encodeValue
import io.github.clasicrando.kdbc.postgresql.stream.PgStream
import io.github.clasicrando.kdbc.postgresql.type.PgTypeCache
import kotlin.reflect.KType
import kotlin.reflect.typeOf

public class PgCopyEncodeBuffer internal constructor(private val typeCache: PgTypeCache) {
    internal val innerBuffer = ByteWriteBuffer(PgStream.WRITE_BUFFER_SIZE - 5)

    public fun <T : Any> encodeValue(value: T?, kType: KType) {
        innerBuffer.encodeValue(value, kType, typeCache)
    }

    public inline fun <reified T : Any> encodeValue(value: T?) {
        encodeValue(value, typeOf<T>())
    }
}
