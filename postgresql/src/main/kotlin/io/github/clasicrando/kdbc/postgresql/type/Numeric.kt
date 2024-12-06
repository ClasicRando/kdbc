package io.github.clasicrando.kdbc.postgresql.type

import io.github.clasicrando.kdbc.postgresql.column.PgValue
import java.math.BigDecimal
import kotlin.reflect.typeOf
import kotlinx.io.Sink

/**
 * Implementation of a [PgTypeDescription] for the [BigDecimal] type. This maps to the `numeric`
 * type in a postgresql database.
 */
internal object BigDecimalTypeDescription :
    PgTypeDescription<BigDecimal>(dbType = PgType.Numeric, kType = typeOf<BigDecimal>()) {
    /**
     * Numeric types are constructed using the internal type [PgNumeric] and encoded to the buffer
     * using [PgNumeric.encodeToBuffer]. To get a [PgNumeric], [PgNumeric.fromBigDecimal] is called
     * to convert the [BigDecimal] value to [PgNumeric].
     */
    override fun encode(value: BigDecimal, buffer: Sink) {
        PgNumeric.fromBigDecimal(value).encodeToBuffer(buffer)
    }

    /**
     * First decode the bytes using [PgNumeric.fromBytes] to get a [PgNumeric] which can be
     * converted to a [BigDecimal] using [PgNumeric.toBigDecimal].
     */
    override fun decodeBytes(value: PgValue.Binary): BigDecimal =
        PgNumeric.fromBytes(value.bytes).toBigDecimal()

    /**
     * When supplied in text format, a [BigDecimal] can be constructed directly from the [String].
     */
    override fun decodeText(value: PgValue.Text): BigDecimal = BigDecimal(value.text)
}
