package com.github.clasicrando.postgresql.column

import com.github.clasicrando.common.column.columnDecodeError

// https://github.com/postgres/postgres/blob/a6c21887a9f0251fa2331ea3ad0dd20b31c4d11d/src/backend/utils/adt/bool.c#L174
val booleanTypeEncoder = PgTypeEncoder<Boolean>(PgType.Bool) { boolean, buffer ->
    buffer.writeByte(if (boolean) 1 else 0)
}

val booleanTypeDecoder = PgTypeDecoder { value ->
    when (value) {
        // https://github.com/postgres/postgres/blob/a6c21887a9f0251fa2331ea3ad0dd20b31c4d11d/src/backend/utils/adt/bool.c#L187
        is PgValue.Binary -> value.bytes.readByte() != 0.toByte()
        // https://github.com/postgres/postgres/blob/a6c21887a9f0251fa2331ea3ad0dd20b31c4d11d/src/backend/utils/adt/bool.c#L126
        is PgValue.Text -> {
            when (value.text) {
                "t" -> true
                "f" -> false
                else -> columnDecodeError<Boolean>(value.typeData)
            }
        }
    }
}
