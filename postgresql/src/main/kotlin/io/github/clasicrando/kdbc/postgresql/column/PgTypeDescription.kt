package io.github.clasicrando.kdbc.postgresql.column

import io.github.clasicrando.kdbc.core.buffer.ByteWriteBuffer
import io.github.clasicrando.kdbc.core.column.ColumnDecodeError
import io.github.clasicrando.kdbc.core.column.columnDecodeError
import kotlin.reflect.KType

/**
 * Interface defining a type that decodes values of the input type [T] into an argument buffer.
 * This interface should not be inherited by an instantiable class but rather an object to define
 * encoders only once and reuse the instance since the object should hold no state and [encode] is
 * a pure function.
 */
interface PgTypeEncodeDescription<in T : Any> {
    /** Encode the [value] into the [buffer] as a collection of [Byte]s */
    fun encode(value: T, buffer: ByteWriteBuffer)
}

/**
 * Interface defining a type that decodes [PgValue]s into the required output type [T]. This
 * interface should not be inherited by an instantiable class but rather an object to define
 * decoders only once and reuse instance since the object should hold no state and [decode] is a
 * pure function.
 */
interface PgTypeDecodeDescription<out T : Any> {
    /**
     * Use the data and context within [value] to return a new instance of [T]. This method should
     * have 2 paths:
     *
     * 1. Decode [PgValue.Binary] using the buffer within
     * 2. Decode [PgValue.Text] using the [String] within
     *
     * @throws ColumnDecodeError If the decode operation fails. In all cases, other exceptions will
     * be caught and this [Exception] will be thrown instead to give more context as to why the
     * operation failed.
     */
    fun decode(value: PgValue): T
}

abstract class PgTypeDescription<T : Any>(
    /**
     * [PgType] that is referenced for this type description as the serialization input and
     * deserialization output
     */
    val pgType: PgType,
    /** Kotlin type of [T] that is recognized by this type description */
    val kType: KType,
) : PgTypeEncodeDescription<T>, PgTypeDecodeDescription<T> {

    /** Decode the bytes provided into the type [T] */
    abstract fun decodeBytes(value: PgValue.Binary): T
    /** Decode the [String] provided into the type [T] */
    abstract fun decodeText(value: PgValue.Text): T

    final override fun decode(value: PgValue): T {
        return when (value) {
            is PgValue.Binary -> {
                try {
                    decodeBytes(value)
                } catch (ex: ColumnDecodeError) {
                    throw ex
                } catch (ex: Exception) {
                    columnDecodeError(
                        kType = kType,
                        type = value.typeData,
                        reason = "Failed to decode bytes for unexpected reason",
                        cause = ex,
                    )
                } finally {
                    value.bytes.reset()
                }
            }
            is PgValue.Text -> try {
                decodeText(value)
            } catch (ex: ColumnDecodeError) {
                throw ex
            } catch (ex: Exception) {
                columnDecodeError(
                    kType = kType,
                    type = value.typeData,
                    reason = "Failed to decode bytes for unexpected reason",
                    cause = ex,
                )
            }
        }
    }
}
