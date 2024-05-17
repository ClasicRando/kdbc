package io.github.clasicrando.kdbc.postgresql.column

import io.github.clasicrando.kdbc.core.buffer.ByteWriteBuffer
import io.github.clasicrando.kdbc.core.column.ColumnDecodeError
import io.github.clasicrando.kdbc.core.column.columnDecodeError
import kotlin.reflect.typeOf

/**
 * Implementation of a [PgTypeDescription] for [Boolean]. This maps to the `boolean` type in a
 * postgresql database
 */
object BoolTypeDescription : PgTypeDescription<Boolean>(
    pgType = PgType.Bool,
    kType = typeOf<Boolean>(),
) {
    /**
     * Simply writes a 1 or 0 for true or false respectively.
     *
     * [pg source code](https://github.com/postgres/postgres/blob/a6c21887a9f0251fa2331ea3ad0dd20b31c4d11d/src/backend/utils/adt/bool.c#L174)
     */
    override fun encode(value: Boolean, buffer: ByteWriteBuffer) {
        buffer.writeByte(if (value) 1 else 0)
    }

    /**
     * Read the first byte and interpret any non-zero byte as true and a 0 as false
     *
     * [pg source code](https://github.com/postgres/postgres/blob/a6c21887a9f0251fa2331ea3ad0dd20b31c4d11d/src/backend/utils/adt/bool.c#L187)
     *
     * @throws ColumnDecodeError if the [String] values provided is not "t" or "f"
     */
    override fun decodeBytes(value: PgValue.Binary): Boolean {
        return value.bytes.readByte() != 0.toByte()
    }

    /**
     * Interpret the [String] value as "t" for true and "f" as false. Otherwise, throw a
     * [ColumnDecodeError].
     *
     * [pg source code](https://github.com/postgres/postgres/blob/a6c21887a9f0251fa2331ea3ad0dd20b31c4d11d/src/backend/utils/adt/bool.c#L126)
     *
     * @throws ColumnDecodeError if the [String] values provided is not "t" or "f"
     */
    override fun decodeText(value: PgValue.Text): Boolean {
        return when (value.text) {
            "t" -> true
            "f" -> false
            else -> columnDecodeError<Boolean>(value.typeData)
        }
    }
}

/**
 * Implementation of an [ArrayTypeDescription] for [Boolean]. This maps to the `boolean[]` type in a
 * postgresql database.
 */
object BoolArrayTypeDescription : ArrayTypeDescription<Boolean>(
    pgType = PgType.BoolArray,
    innerType = BoolTypeDescription,
)
