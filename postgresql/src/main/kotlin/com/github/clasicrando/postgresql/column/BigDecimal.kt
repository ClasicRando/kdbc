package com.github.clasicrando.postgresql.column

import com.github.clasicrando.postgresql.type.PgNumeric
import java.math.BigDecimal

/**
 * Implementation of a [PgTypeEncoder] for the [BigDecimal] type. This maps to the `numeric` type
 * in a postgresql database. Numeric types are constructed using the internal type [PgNumeric] and
 * encoded to the buffer using [PgNumeric.encodeToBuffer]. To get a [PgNumeric],
 * [PgNumeric.fromBigDecimal] is called to convert the [BigDecimal] value to [PgNumeric].
 */
internal val bigDecimalTypeEncoder = PgTypeEncoder<BigDecimal>(PgType.Numeric) { value, buffer ->
    PgNumeric.fromBigDecimal(value)
        .encodeToBuffer(buffer)
}

/**
 * Implementation of a [PgTypeDecoder] for the [BigDecimal] type. This maps to the `numeric` type
 * in a postgresql database.
 *
 * ### Binary
 * When supplied in the binary format, [PgNumeric.fromBytes] is used to get a [PgNumeric] which can
 * be converted to a [BigDecimal] using [PgNumeric.toBigDecimal].
 *
 * ### Text
 * When supplied in text format, a [BigDecimal] can be constructed directly from the [String].
 */
internal val bigDecimalTypeDecoder = PgTypeDecoder { value ->
    when (value) {
        is PgValue.Binary -> PgNumeric.fromBytes(value.bytes).toBigDecimal()
        is PgValue.Text -> BigDecimal(value.text)
    }
}
