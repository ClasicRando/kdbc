package io.github.clasicrando.kdbc.postgresql.type

import com.ionspin.kotlin.bignum.decimal.BigDecimal
import com.ionspin.kotlin.bignum.integer.BigInteger
import io.github.clasicrando.kdbc.core.buffer.ByteReadBuffer
import io.github.clasicrando.kdbc.core.buffer.ByteWriteBuffer
import io.github.clasicrando.kdbc.core.toBigDecimalWithTraditionalScale
import io.github.clasicrando.kdbc.core.traditionalScale
import io.github.clasicrando.kdbc.postgresql.type.PgNumeric.NAN
import kotlin.math.max
import kotlin.math.pow

internal const val SIGN_NAN: Short = 0xC0
internal const val SIGN_POSITIVE: Short = 0x0000
internal const val SIGN_NEGATIVE: Short = 0x4000

/**
 * Type that directly maps to the `numeric` type in a postgresql database. A `numeric` value can be
 * either [NAN] or an actual number with specific properties describing the storage of the number.
 * This type is useful as an intermediary that can be easily written to the arguments buffer and
 * read from a row buffer. It also includes utility methods to convert to and from [BigDecimal]
 * which is the type that most applications will use to represent large decimal numbers.
 */
internal sealed class PgNumeric {
    /** Any numeric value that is NAN. A special number in the postgresql */
    data object NAN : PgNumeric()

    /**
     * Type that reflects the internal storage and transit format of a postgresql numeric type.
     *
     * @param sign Sign of the number. 1 of 3 values: 0x0000 -> positive, 0x4000 -> negative, 0xC0
     * -> NAN (not representable by [BigDecimal] but implemented for mapping to postgres type)
     * @param digits Base 10_000 values that represent the number
     * @param weight Represents the number of digits before the decimal point plus 1. [weight] can
     * be less than zero.
     * @param scale Represents the number of digits after the decimal point. This must always be
     * greater than 0.
     */
    class Number(
        val sign: Short,
        val digits: ShortArray,
        val weight: Short,
        val scale: Short,
    ) : PgNumeric()

    /**
     * Encode this [PgNumeric] into the argument [buffer] in the following format and order:
     *
     * 1. [Short] - number of digits within the number
     * 2. [Short] - weight of the number
     * 3. [Short] - sign of the number
     * 4. [Short] - scale of the number
     * 5. Dynamic - all digits encoded as [Short] values (base 10_000), count must match the first
     * number encoded
     *
     * For [PgNumeric.NAN], all values are 0 (with no digits) expect for the sign which is 0xC0.
     *
     * [pg source code](https://github.com/postgres/postgres/blob/a6c21887a9f0251fa2331ea3ad0dd20b31c4d11d/src/backend/utils/adt/numeric.c#L1068)
     */
    internal fun encodeToBuffer(buffer: ByteWriteBuffer) {
        when (this) {
            NAN -> {
                buffer.writeShort(0)
                buffer.writeShort(0)
                buffer.writeShort(SIGN_NAN)
                buffer.writeShort(0)
            }
            is Number -> {
                buffer.writeShort(this.digits.size.toShort())
                buffer.writeShort(this.weight)
                buffer.writeShort(this.sign)
                buffer.writeShort(this.scale)
                for (digit in this.digits) {
                    buffer.writeShort(digit)
                }
            }
        }
    }

    /**
     * !!Disclaimer!!
     * This code is taken mostly as-is from the postgres jdbc driver (with some modifications
     * during the conversion to kotlin code). I hope to find a better cross-platform solution
     * to remove this java dependency.
     *
     * [pgjdbc code](https://github.com/pgjdbc/pgjdbc/blob/a4089461cacc5e6f0168ab95bf2ff7d253de8336/pgjdbc/src/main/java/org/postgresql/util/ByteConverter.java#L124)
     */
    internal fun toBigDecimal(): BigDecimal {
        val number = when (this) {
            NAN -> error("BigDecimal does not support Nan values")
            is Number -> this
        }

        //0 based number of 4 decimal digits (i.e. 2-byte shorts) before the decimal
        //a value <= 0 indicates an absolute value < 1.
        var weight: Short = number.weight

        require((scale.toInt() and NUMERIC_DSCALE_MASK) == scale.toInt()) {
            "invalid scale in \"numeric\" value"
        }

        if (number.digits.isEmpty()) {
            return BigInteger.ZERO.toBigDecimalWithTraditionalScale(scale = number.scale)
                .scale(0)
        }

        var idx = 0

        var d: Short = number.digits[idx]

        //if the absolute value is (0, 1), then leading '0' values
        //do not matter for the unscaledInt, but trailing 0s do
        if (weight < 0) {
            assert(scale > 0)
            var effectiveScale = scale.toInt()
            //adjust weight to determine how many leading 0s after the decimal
            //before the provided values/digits actually begin
            ++weight
            if (weight < 0) {
                effectiveScale += 4 * weight
            }

            var i = 1
            //typically there should not be leading 0 short values, as it is more
            //efficient to represent that in the weight value
            while (i < number.digits.size && d.toInt() == 0) {
                //each leading 0 value removes 4 from the effective scale
                effectiveScale -= 4
                idx++
                d = number.digits[idx]
                i++
            }

            assert(effectiveScale > 0)
            if (effectiveScale >= 4) {
                effectiveScale -= 4
            } else {
                //an effective scale of less than four means that the value d
                //has trailing 0s which are not significant
                //so we divide by the appropriate power of 10 to reduce those
                d = (d / INT_TEN_POWERS[4 - effectiveScale]).toShort()
                effectiveScale = 0
            }
            //defer moving to BigInteger as long as possible
            //operations on the long are much faster
            var unscaledBI: BigInteger? = null
            var unscaledInt = d.toLong()
            while (i < number.digits.size) {
                if (i == 4 && effectiveScale > 2) {
                    unscaledBI = BigInteger.fromLong(unscaledInt)
                }
                idx++
                d = number.digits[idx]
                //if effective scale is at least 4, then all 4 digits should be used
                //and the existing number needs to be shifted 4
                if (effectiveScale >= 4) {
                    if (unscaledBI == null) {
                        unscaledInt *= 10000
                    } else {
                        unscaledBI = unscaledBI.multiply(BI_TEN_THOUSAND)
                    }
                    effectiveScale -= 4
                } else {
                    //if effective scale is less than 4, then only shift left based on remaining scale
                    if (unscaledBI == null) {
                        unscaledInt *= INT_TEN_POWERS[effectiveScale]
                    } else {
                        unscaledBI = unscaledBI.multiply(tenPower(effectiveScale))
                    }
                    //and d needs to be shifted to the right to only get correct number of
                    //significant digits
                    d = (d / INT_TEN_POWERS[4 - effectiveScale]).toShort()
                    effectiveScale = 0
                }
                if (unscaledBI == null) {
                    unscaledInt += d.toLong()
                } else {
                    if (d.toInt() != 0) {
                        unscaledBI = unscaledBI.add(BigInteger.fromShort(d))
                    }
                }
                i++
            }
            //now we need BigInteger to create BigDecimal
            if (unscaledBI == null) {
                unscaledBI = BigInteger.fromLong(unscaledInt)
            }
            //if there is remaining effective scale, apply it here
            if (effectiveScale > 0) {
                unscaledBI = unscaledBI.multiply(tenPower(effectiveScale))
            }
            if (sign == SIGN_NEGATIVE) {
                unscaledBI = unscaledBI.negate()
            }

            return unscaledBI.toBigDecimalWithTraditionalScale(scale = number.scale)
        }

        //if there is no scale, then shorts are the unscaled int
        if (scale.toInt() == 0) {
            //defer moving to BigInteger as long as possible
            //operations on the long are much faster
            var unscaledBI: BigInteger? = null
            var unscaledInt = d.toLong()
            //loop over all of the len shorts to process as the unscaled int
            for (i in 1..<number.digits.size) {
                if (i == 4) {
                    unscaledBI = BigInteger.fromLong(unscaledInt)
                }
                idx++
                d = number.digits[idx]
                if (unscaledBI == null) {
                    unscaledInt *= 10000
                    unscaledInt += d.toLong()
                } else {
                    unscaledBI = unscaledBI.multiply(BI_TEN_THOUSAND)
                    if (d.toInt() != 0) {
                        unscaledBI = unscaledBI.add(BigInteger.fromShort(d))
                    }
                }
            }
            //now we need BigInteger to create BigDecimal
            if (unscaledBI == null) {
                unscaledBI = BigInteger.fromLong(unscaledInt)
            }
            if (sign == SIGN_NEGATIVE) {
                unscaledBI = unscaledBI.negate()
            }
            //the difference between len and weight (adjusted from 0 based) becomes the scale for BigDecimal
            val bigDecScale = (number.digits.size - (weight + 1)) * 4
            //string representation always results in a BigDecimal with scale of 0
            //the binary representation, where weight and len can infer trailing 0s, can result in a negative scale
            //to produce a consistent BigDecimal, we return the equivalent object with scale set to 0
            return if (bigDecScale == 0) {
                BigDecimal.fromBigInteger(unscaledBI)
            } else {
                unscaledBI.toBigDecimalWithTraditionalScale(scale = number.scale)
                    .scale(0)
            }
        }

        //defer moving to BigInteger as long as possible
        //operations on the long are much faster
        var unscaledBI: BigInteger? = null
        var unscaledInt = d.toLong()
        //weight and scale as defined by postgresql are a bit different than how BigDecimal treats scale
        //maintain the effective values to massage as we process through values
        var effectiveWeight = weight.toInt()
        var effectiveScale = scale.toInt()
        for (i in 1..<number.digits.size) {
            if (i == 4) {
                unscaledBI = BigInteger.fromLong(unscaledInt)
            }
            idx++
            d = number.digits[idx]
            //first process effective weight down to 0
            if (effectiveWeight > 0) {
                --effectiveWeight
                if (unscaledBI == null) {
                    unscaledInt *= 10000
                } else {
                    unscaledBI = unscaledBI.multiply(BI_TEN_THOUSAND)
                }
            } else if (effectiveScale >= 4) {
                //if effective scale is at least 4, then all 4 digits should be used
                //and the existing number needs to be shifted 4
                effectiveScale -= 4
                if (unscaledBI == null) {
                    unscaledInt *= 10000
                } else {
                    unscaledBI = unscaledBI.multiply(BI_TEN_THOUSAND)
                }
            } else {
                //if effective scale is less than 4, then only shift left based on remaining scale
                if (unscaledBI == null) {
                    unscaledInt *= INT_TEN_POWERS[effectiveScale]
                } else {
                    unscaledBI = unscaledBI.multiply(tenPower(effectiveScale))
                }
                //and d needs to be shifted to the right to only get correct number of
                //significant digits
                d = (d / INT_TEN_POWERS[4 - effectiveScale]).toShort()
                effectiveScale = 0
            }
            if (unscaledBI == null) {
                unscaledInt += d.toLong()
            } else {
                if (d.toInt() != 0) {
                    unscaledBI = unscaledBI.add(BigInteger.fromLong(d.toLong()))
                }
            }
        }

        //now we need BigInteger to create BigDecimal
        if (unscaledBI == null) {
            unscaledBI = BigInteger.fromLong(unscaledInt)
        }
        //if there is remaining weight, apply it here
        if (effectiveWeight > 0) {
            unscaledBI = unscaledBI.multiply(tenPower(effectiveWeight * 4))
        }
        //if there is remaining effective scale, apply it here
        if (effectiveScale > 0) {
            unscaledBI = unscaledBI.multiply(tenPower(effectiveScale))
        }
        if (sign == SIGN_NEGATIVE) {
            unscaledBI = unscaledBI.negate()
        }
        return unscaledBI.toBigDecimalWithTraditionalScale(scale = number.scale)
    }

    companion object {
        private const val NUMERIC_DSCALE_MASK = 0x00003FFF
        private val INT_TEN_POWERS = IntArray(6) { 10.0.pow(it).toInt() }
        private val BI_TEN_POWERS = Array(32) { BigInteger.TEN.pow(it) }
        private val BI_MAX_LONG = BigInteger.fromLong(Long.MAX_VALUE)
        private val BI_TEN_THOUSAND = BigInteger.fromInt(10000)

        private fun tenPower(exponent: Int): BigInteger {
            return BI_TEN_POWERS.getOrElse(exponent) { BigInteger.TEN.pow(exponent) }
        }

        /**
         * Decode a [PgNumeric] from the [buffer] supplied. Reads:
         *
         * 1. [Short] - the number of digits to follow later
         * 2. [Short] - the weight of the number
         * 3. [Short] - the sign of the number
         * 4. [Short] - scale of the number
         * 5. Dynamic - all digits encoded as [Short] values (base 10_000), count must match the
         * first number decoded
         *
         * Depending on the third [Short] read (the sign value), the number is either read as a
         * [NAN] or all the values are packed into a [Number].
         *
         * [pg source code](https://github.com/postgres/postgres/blob/a6c21887a9f0251fa2331ea3ad0dd20b31c4d11d/src/backend/utils/adt/numeric.c#L1153)
         */
        internal fun fromBytes(buffer: ByteReadBuffer): PgNumeric {
            val numDigits = buffer.readShort()
            val weight = buffer.readShort()
            val sign = buffer.readShort()
            val scale = buffer.readShort()

            return if (sign == SIGN_NAN) {
                NAN
            } else {
                val digits = ShortArray(numDigits.toInt()) { buffer.readShort() }
                Number(
                    sign = sign,
                    scale = scale,
                    weight = weight,
                    digits = digits,
                )
            }
        }

        /**
         * !!Disclaimer!!
         * This code is taken mostly as-is from the postgres jdbc driver (with some modifications
         * during the conversion to kotlin code). I hope to find a better cross-platform solution
         * to remove this java dependency.
         *
         * [pgjdbc code](https://github.com/pgjdbc/pgjdbc/blob/a4089461cacc5e6f0168ab95bf2ff7d253de8336/pgjdbc/src/main/java/org/postgresql/util/ByteConverter.java#L382)
         */
        internal fun fromBigDecimal(bigDecimal: BigDecimal): PgNumeric {
            val shorts = mutableListOf<Short>()
            var unscaled = bigDecimal.significand.abs()
            var scale = bigDecimal.traditionalScale.toInt()
            if (unscaled == BigInteger.ZERO) {
                return Number(
                    sign = SIGN_POSITIVE,
                    scale = max(scale, 0).toShort(),
                    weight = 0,
                    digits = shortArrayOf(),
                )
            }

            var weight = -1
            if (scale <= 0) {
                if (scale < 0) {
                    scale = kotlin.math.abs(scale)
                    weight += scale / 4
                    val mod = scale % 4
                    unscaled *= tenPower(mod)
                }
                while (unscaled > BI_MAX_LONG) {
                    val pair = unscaled.divideAndRemainder(BI_TEN_THOUSAND)
                    unscaled = pair.first
                    val short = pair.second.shortValue()
                    if (short != 0.toShort() || shorts.isNotEmpty()) {
                        shorts.add(short)
                    }
                    ++weight
                }
                var unscaledLong = unscaled.longValue()
                do {
                    val short = (unscaledLong % 10000).toShort()
                    if (short != 0.toShort() || shorts.isNotEmpty()) {
                        shorts.add(short)
                    }
                    unscaledLong /= 10000L
                    ++weight
                } while (unscaledLong != 0L)

                val lastIndex = shorts.size - 1
                return Number(
                    sign = when {
                        bigDecimal.signum() == -1 -> SIGN_NEGATIVE
                        else -> SIGN_POSITIVE
                    },
                    scale = 0,
                    weight = weight.toShort(),
                    digits = ShortArray(shorts.size) { shorts[lastIndex - it] },
                )
            }

            var (wholes, decimal) = unscaled.divideAndRemainder(tenPower(scale))
            weight = -1
            if (BigInteger.ZERO != decimal) {
                val mod = scale % 4
                var segments = scale / 4
                if (mod != 0) {
                    decimal *= tenPower(4 - mod)
                    ++segments
                }
                do {
                    val pair = decimal.divideAndRemainder(BI_TEN_THOUSAND)
                    decimal = pair.first
                    val short = pair.second.shortValue()
                    if (short != 0.toShort() || shorts.isNotEmpty()) {
                        shorts.add(short)
                    }
                    --segments
                } while (BigInteger.ZERO != decimal)

                if (BigInteger.ZERO == wholes) {
                    weight -= segments
                } else {
                    for (i in 0..<segments) {
                        shorts.add(0)
                    }
                }
            }
            while (BigInteger.ZERO != wholes) {
                ++weight
                val pair = wholes.divideAndRemainder(BI_TEN_THOUSAND)
                wholes = pair.first
                val short = pair.second.shortValue()
                if (short != 0.toShort() || shorts.isNotEmpty()) {
                    shorts.add(short)
                }
            }

            val lastIndex = shorts.size - 1
            return Number(
                sign = when {
                    bigDecimal.signum() == -1 -> SIGN_NEGATIVE
                    else -> SIGN_POSITIVE
                },
                scale = max(scale, 0).toShort(),
                weight = weight.toShort(),
                digits = ShortArray(shorts.size) { shorts[lastIndex - it] },
            )
        }
    }
}
