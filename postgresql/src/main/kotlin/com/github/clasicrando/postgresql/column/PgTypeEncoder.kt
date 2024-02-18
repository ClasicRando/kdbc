package com.github.clasicrando.postgresql.column

import com.github.clasicrando.common.buffer.ArrayWriteBuffer
import kotlin.reflect.KType
import kotlin.reflect.typeOf

interface PgTypeEncoder<in T : Any> {
    fun encode(value: T, buffer: ArrayWriteBuffer)

    val encodeType: KType

    var pgType: PgType

    val compatibleTypes: Array<PgType>? get() = null
}

inline fun <reified T : Any> PgTypeEncoder(
    pgType: PgType,
    compatibleTypes: Array<PgType>? = null,
    crossinline block: (value: T, buffer: ArrayWriteBuffer) -> Unit,
): PgTypeEncoder<T> = object : PgTypeEncoder<T> {
    override fun encode(value: T, buffer: ArrayWriteBuffer) {
        block(value, buffer)
    }

    override val encodeType: KType = typeOf<T>()

    override var pgType: PgType = pgType

    override val compatibleTypes: Array<PgType>? = compatibleTypes
}
