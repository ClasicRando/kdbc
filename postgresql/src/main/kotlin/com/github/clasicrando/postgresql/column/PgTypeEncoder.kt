package com.github.clasicrando.postgresql.column

import com.github.clasicrando.postgresql.statement.PgArguments
import kotlin.reflect.KType
import kotlin.reflect.typeOf

interface PgTypeEncoder<in T : Any> {
    fun encode(value: T, buffer: PgArguments)

    val encodeType: KType

    var pgType: PgType

    val compatibleTypes: Array<PgType>? get() = null
}

inline fun <reified T : Any> PgTypeEncoder(
    pgType: PgType,
    compatibleTypes: Array<PgType>? = null,
    crossinline block: (value: T, buffer: PgArguments) -> Unit,
): PgTypeEncoder<T> = object : PgTypeEncoder<T> {
    override fun encode(value: T, buffer: PgArguments) {
        block(value, buffer)
    }

    override val encodeType: KType = typeOf<T>()

    override var pgType: PgType = pgType

    override val compatibleTypes: Array<PgType>? = compatibleTypes
}
