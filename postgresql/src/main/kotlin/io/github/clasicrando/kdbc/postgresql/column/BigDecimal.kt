package io.github.clasicrando.kdbc.postgresql.column

import com.ionspin.kotlin.bignum.decimal.BigDecimal
import io.github.clasicrando.kdbc.core.buffer.ByteWriteBuffer
import io.github.clasicrando.kdbc.postgresql.type.PgNumeric
import kotlin.reflect.typeOf

/**
 * Implementation of a [PgTypeDescription] for the [BigDecimal] type. This maps to the `numeric`
 * type in a postgresql database.
 */
internal object NumericTypeDescription : PgTypeDescription<BigDecimal>(
    pgType = PgType.Numeric,
    kType = typeOf<BigDecimal>(),
) {
    /**
     * Numeric types are constructed using the internal type [PgNumeric] and encoded to the buffer
     * using [PgNumeric.encodeToBuffer]. To get a [PgNumeric], [PgNumeric.fromBigDecimal] is called
     * to convert the [BigDecimal] value to [PgNumeric].
     */
    override fun encode(value: BigDecimal, buffer: ByteWriteBuffer) {
        PgNumeric.fromBigDecimal(value)
            .encodeToBuffer(buffer)
    }

    /**
     * First decode the bytes using [PgNumeric.fromBytes] to get a [PgNumeric] which can be
     * converted to a [BigDecimal] using [PgNumeric.toBigDecimal].
     */
    override fun decodeBytes(value: PgValue.Binary): BigDecimal {
        return PgNumeric.fromBytes(value.bytes).toBigDecimal()
    }

    /**
     * When supplied in text format, a [BigDecimal] can be constructed directly from the [String].
     */
    override fun decodeText(value: PgValue.Text): BigDecimal {
        return BigDecimal.parseString(value.text)
    }
}

/**
 * Implementation of an [ArrayTypeDescription] for [BigDecimal]. This maps to the `numeric[]` type
 * in a postgresql database.
 */
internal object NumericArrayTypeDescription : ArrayTypeDescription<BigDecimal>(
    pgType = PgType.NumericArray,
    innerType = NumericTypeDescription,
)
