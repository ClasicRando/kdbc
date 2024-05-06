package io.github.clasicrando.kdbc.postgresql.column

import io.github.clasicrando.kdbc.core.buffer.ByteWriteBuffer
import io.github.clasicrando.kdbc.core.column.ColumnDecodeError
import kotlin.reflect.KType

interface PgTypeAssociated {
    /**
     * Values passed to this encoder are generally made available to the database in this [PgType].
     */
    val pgType: PgType
}

/**
 * Interface defining a type that decodes values of the input type [T] into an argument buffer.
 * This interface should not be inherited by an instantiable class but rather an object to define
 * encoders only once and reuse the instance since the object should hold no state and [encode] is
 * a pure function.
 */
interface PgTypeEncodeDescription<in T : Any> : PgTypeAssociated {
    /** Encode the [value] into the [buffer] as a collection of [Byte]s */
    fun encode(value: T, buffer: ByteWriteBuffer)
}

/**
 * Interface defining a type that decodes [PgValue]s into the required output type [T]. This
 * interface should not be inherited by an instantiable class but rather an object to define
 * decoders only once and reuse instance since the object should hold no state and [decode] is a
 * pure function.
 */
interface PgTypeDecodeDescription<out T : Any>: PgTypeAssociated {
    /**  */
    fun decodeBytes(value: PgValue.Binary): T
    /**  */
    fun decodeText(value: PgValue.Text): T
}

abstract class PgTypeDescription<T : Any>(
    final override val pgType: PgType,
    val kType: KType,
) : PgTypeEncodeDescription<T>, PgTypeDecodeDescription<T> {
    /**
     * Use the data and context within [value] to return a new instance of [T]. This method should
     * generally have 2 paths:
     *
     * 1. Decode [PgValue.Binary] using the bytes view within
     * 2. Decode [PgValue.Text] using the [String] within
     *
     * In some cases the 2 cases can converge into a single branch of logic. For example, [Enum]
     * types are always text so the binary version is just a [String] encoded using the client
     * encoding.
     *
     * @throws ColumnDecodeError if the decode operation fails. In most cases, other exceptions
     * will be caught and this [Exception] will be thrown instead to give more context as to why
     * the operation failed.
     */
    fun decode(value: PgValue): T {
        return when (value) {
            is PgValue.Binary -> {
                try {
                    decodeBytes(value)
                } finally {
                    value.bytes.reset()
                }
            }
            is PgValue.Text -> decodeText(value)
        }
    }
}
