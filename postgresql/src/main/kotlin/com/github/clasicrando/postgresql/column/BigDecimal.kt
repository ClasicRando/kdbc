package com.github.clasicrando.postgresql.column

import com.github.clasicrando.common.buffer.writeShort
import com.github.clasicrando.postgresql.type.PgNumeric
import com.github.clasicrando.postgresql.type.SIGN_NAN
import java.math.BigDecimal

// https://github.com/postgres/postgres/blob/a6c21887a9f0251fa2331ea3ad0dd20b31c4d11d/src/backend/utils/adt/numeric.c#L1068
internal val bigDecimalTypeEncoder = PgTypeEncoder<BigDecimal>(PgType.Numeric) { value, buffer ->
    when (val numeric = PgNumeric.fromBigDecimal(value)) {
        PgNumeric.NAN -> {
            buffer.writeShort(0)
            buffer.writeShort(0)
            buffer.writeShort(SIGN_NAN)
            buffer.writeShort(0)
        }
        is PgNumeric.Number -> {
            buffer.writeShort(numeric.digits.size.toShort())
            buffer.writeShort(numeric.weight)
            buffer.writeShort(numeric.sign)
            buffer.writeShort(numeric.scale)
            for (digit in numeric.digits) {
                buffer.writeShort(digit)
            }
        }
    }
}

internal val bigDecimalTypeDecoder = PgTypeDecoder { value ->
    when (value) {
        is PgValue.Binary -> PgNumeric.fromBytes(value.bytes).toBigDecimal()
        is PgValue.Text -> BigDecimal(value.text)
    }
}
