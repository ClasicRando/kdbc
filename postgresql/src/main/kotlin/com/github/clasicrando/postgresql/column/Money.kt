package com.github.clasicrando.postgresql.column

import com.github.clasicrando.common.buffer.writeLong
import com.github.clasicrando.postgresql.type.PgMoney

val moneyTypeEncoder = PgTypeEncoder<PgMoney>(PgType.Money) { value, buffer ->
    buffer.writeLong(value.integer)
}

val moneyTypeDecoder = PgTypeDecoder { value ->
    when (value) {
        is PgValue.Binary -> PgMoney(value.bytes.readLong())
        is PgValue.Text -> PgMoney(value.text)
    }
}
