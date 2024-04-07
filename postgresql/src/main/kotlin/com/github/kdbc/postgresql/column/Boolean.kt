package com.github.kdbc.postgresql.column

import com.github.kdbc.core.column.ColumnDecodeError
import com.github.kdbc.core.column.columnDecodeError

/**
 * Implementation of [PgTypeEncoder] for [Boolean]. This maps to the `boolean` type in a postgresql
 * database. Simply writes a 1 or 0 for true or false respectively.
 *
 * [pg source code](https://github.com/postgres/postgres/blob/a6c21887a9f0251fa2331ea3ad0dd20b31c4d11d/src/backend/utils/adt/bool.c#L174)
 */
val booleanTypeEncoder = PgTypeEncoder<Boolean>(PgType.Bool) { boolean, buffer ->
    buffer.writeByte(if (boolean) 1 else 0)
}

/**
 * Implementation of [PgTypeDecoder] for [Boolean]. This maps to the `boolean` type in a postgresql
 * database.
 *
 * ### Binary
 * Read the first byte and interpret any non-zero byte as true and a 0 as false
 *
 * ### Text
 * Interpret the [String] value as "t" for true and "f" as false. Otherwise, throw a
 * [ColumnDecodeError].
 *
 * [pg source code binary](https://github.com/postgres/postgres/blob/a6c21887a9f0251fa2331ea3ad0dd20b31c4d11d/src/backend/utils/adt/bool.c#L187)
 * [pg source code text](https://github.com/postgres/postgres/blob/a6c21887a9f0251fa2331ea3ad0dd20b31c4d11d/src/backend/utils/adt/bool.c#L126)
 *
 * @throws ColumnDecodeError if the [String] values provided is not "t" or "f"
 */
val booleanTypeDecoder = PgTypeDecoder { value ->
    when (value) {
        is PgValue.Binary -> value.bytes.readByte() != 0.toByte()
        is PgValue.Text -> {
            when (value.text) {
                "t" -> true
                "f" -> false
                else -> columnDecodeError<Boolean>(value.typeData)
            }
        }
    }
}
