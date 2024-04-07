package com.github.kdbc.postgresql.column

import com.github.kdbc.core.buffer.ByteWriteBuffer
import kotlin.reflect.KType
import kotlin.reflect.typeOf

/**
 * Interface defining a type that decodes values of the input type [T] into an argument buffer.
 * This interface should not be inherited by an instantiable class but rather an object to define
 * encoders only once and reuse the instance since the object should hold no state and [encode] is
 * a pure function.
 */
interface PgTypeEncoder<in T : Any> {
    /** Encode the [value] into the [buffer] as a collection of [Byte]s */
    fun encode(value: T, buffer: ByteWriteBuffer)

    /**
     * [KType]s that can be encoded using this encoder. This is used to map [KType]s to an encoder
     * so when a value needs to be encoded, the encoder can be looked up using the [KType] of that
     * value.
     */
    val encodeTypes: List<KType>

    /**
     * Values passed to this encoder are generally made available to the database in this [PgType].
     * This type should be able to be coerced into the [compatibleTypes] by the sever backend.
     */
    var pgType: PgType

    /** Optional [PgType]s that can be coerced into from [pgType] */
    val compatibleTypes: Array<PgType>? get() = null
}

/**
 * Function to create new anonymous [PgTypeEncoder] instances. This is easier than implementing
 * [PgTypeEncoder] on an object declaration since the reified parameter can be used to auto
 * generate 1 of the parameters but also supply the encode method as an inlined lambda.
 */
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
