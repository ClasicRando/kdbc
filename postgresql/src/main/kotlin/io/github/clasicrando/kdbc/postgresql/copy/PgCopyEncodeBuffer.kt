package io.github.clasicrando.kdbc.postgresql.copy

import io.github.clasicrando.kdbc.core.buffer.ByteListWriteBuffer
import io.github.clasicrando.kdbc.core.buffer.ByteWriteBuffer
import io.github.clasicrando.kdbc.core.buffer.writeLengthPrefixed
import io.github.clasicrando.kdbc.core.exceptions.KdbcException
import io.github.clasicrando.kdbc.postgresql.type.PgTypeCache
import kotlin.reflect.KType
import kotlin.reflect.typeOf

class PgCopyEncodeBuffer internal constructor(
    private val typeCache: PgTypeCache,
) : AutoCloseable {
    internal val innerBuffer: ByteWriteBuffer = ByteListWriteBuffer()
    private val innerTypes = mutableListOf<Int>()
    val types: List<Int> get() = innerTypes

    private fun <T : Any> encodeNonNullValue(
        value: T,
        kType: KType,
    ) {
        val description =
            typeCache.getTypeDescription<T>(kType)
                ?: throw KdbcException("Could not find type description for $kType")
        innerBuffer.writeLengthPrefixed {
            description.encode(value, innerBuffer)
        }
    }

    fun <T : Any> encodeValue(
        value: T?,
        kType: KType,
    ) {
        if (value == null) {
            innerBuffer.writeInt(-1)
            return
        }
        encodeNonNullValue(value, kType)
    }

    inline fun <reified T : Any> encodeValue(value: T?) {
        encodeValue(value, typeOf<T>())
    }

    override fun close() {
        innerBuffer.close()
    }
}
