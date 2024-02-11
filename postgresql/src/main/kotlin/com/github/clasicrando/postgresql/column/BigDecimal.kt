package com.github.clasicrando.postgresql.column

import com.github.clasicrando.common.column.columnDecodeError
import com.github.clasicrando.postgresql.type.PgNumeric
import com.github.clasicrando.postgresql.type.SIGN_NAN
import io.ktor.utils.io.core.writeShort
import java.math.BigDecimal

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
            buffer.writeShort(numeric.sign.value)
            buffer.writeShort(numeric.scale)
            for (digit in numeric.digits) {
                buffer.writeShort(digit)
            }
        }
    }
}

internal val bigDecimalTypeDecoder = PgTypeDecoder { value ->
    when (value) {
        is PgValue.Binary -> {
            val numDigits = value.bytes.readShort()
            val weight = value.bytes.readShort()
            val sign = value.bytes.readShort()
            val scale = value.bytes.readShort()

            val numeric = if (sign == SIGN_NAN) {
                PgNumeric.NAN
            } else {
                val digits = ShortArray(numDigits.toInt()) { value.bytes.readShort() }
                PgNumeric.Number(
                    sign = PgNumeric.PgNumericSign
                        .entries
                        .firstOrNull { it.value == sign }
                        ?: columnDecodeError<BigDecimal>(value.typeData),
                    scale = scale,
                    weight = weight,
                    digits = digits,
                )
            }
            numeric.toBigDecimal()
        }
        is PgValue.Text -> BigDecimal(value.text)
    }
}
