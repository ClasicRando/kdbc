package io.github.clasicrando.kdbc.postgresql.copy

import io.github.clasicrando.kdbc.core.exceptions.KdbcException
import io.github.clasicrando.kdbc.postgresql.buffer.writeLengthPrefixedInt
import io.github.clasicrando.kdbc.postgresql.type.PgTypeCache
import kotlinx.io.Buffer
import kotlin.reflect.KType
import kotlin.reflect.typeOf

class PgCopyEncodeBuffer internal constructor(
    private val typeCache: PgTypeCache,
) : AutoCloseable {
    internal val innerBuffer = Buffer()
    private val innerTypes = mutableListOf<Int>()
    val types: List<Int> get() = innerTypes

    private fun <T : Any> encodeNonNullValue(
        value: T,
        kType: KType,
    ) {
        val description =
            typeCache.getTypeDescription<T>(kType)
                ?: throw KdbcException("Could not find type description for $kType")
        innerBuffer.writeLengthPrefixedInt {
            description.encode(value, this)
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
