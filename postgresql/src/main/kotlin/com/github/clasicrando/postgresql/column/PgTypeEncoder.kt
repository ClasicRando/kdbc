package com.github.clasicrando.postgresql.column

import com.github.clasicrando.common.buffer.ByteWriteBuffer
import kotlin.reflect.KType
import kotlin.reflect.typeOf

interface PgTypeEncoder<in T : Any> {
    fun encode(value: T, buffer: ByteWriteBuffer)

    val encodeTypes: List<KType>

    var pgType: PgType

    val compatibleTypes: Array<PgType>? get() = null
}

inline fun <reified T : Any> PgTypeEncoder(
    pgType: PgType,
    compatibleTypes: Array<PgType>? = null,
    encodeTypes: List<KType>? = null,
    crossinline block: (value: T, buffer: ByteWriteBuffer) -> Unit,
): PgTypeEncoder<T> = object : PgTypeEncoder<T> {
    override fun encode(value: T, buffer: ByteWriteBuffer) {
        block(value, buffer)
    }

    override val encodeTypes: List<KType> = encodeTypes ?: listOf(typeOf<T>())

    override var pgType: PgType = pgType

    override val compatibleTypes: Array<PgType>? = compatibleTypes
}
