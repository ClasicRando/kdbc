package com.github.clasicrando.postgresql.column

import com.github.clasicrando.common.column.columnDecodeError

val booleanTypeEncoder = PgTypeEncoder<Boolean>(PgType.Bool) { boolean, buffer ->
    buffer.writeByte(if (boolean) 1 else 0)
}

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
