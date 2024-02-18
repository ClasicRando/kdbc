package com.github.clasicrando.postgresql.type

import com.github.clasicrando.common.buffer.ReadBuffer
import com.github.clasicrando.common.buffer.readShort
import java.math.BigDecimal
import java.math.BigInteger
import kotlin.math.max
import kotlin.math.pow


internal const val SIGN_NAN: Short = 0xC0
internal const val SIGN_POSITIVE: Short = 0x0000
internal const val SIGN_NEGATIVE: Short = 0x4000

internal sealed class PgNumeric {
    data object NAN : PgNumeric()
    class Number(
        val sign: Short,
        val digits: ShortArray,
        val weight: Short,
        val scale: Short,
    ) : PgNumeric()

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

        require((scale.toInt() and NUMERIC_DSCALE_MASK) == scale.toInt()) { "invalid scale in \"numeric\" value" }

        if (number.digits.isEmpty()) {
            return BigDecimal(BigInteger.ZERO, scale.toInt())
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
                    unscaledBI = BigInteger.valueOf(unscaledInt)
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
                        unscaledBI = unscaledBI.add(BigInteger.valueOf(d.toLong()))
                    }
                }
                i++
            }
            //now we need BigInteger to create BigDecimal
            if (unscaledBI == null) {
                unscaledBI = BigInteger.valueOf(unscaledInt)
            }
            //if there is remaining effective scale, apply it here
            if (effectiveScale > 0) {
                unscaledBI = unscaledBI!!.multiply(tenPower(effectiveScale))
            }
            if (sign == SIGN_NEGATIVE) {
                unscaledBI = unscaledBI!!.negate()
            }

            return BigDecimal(unscaledBI, scale.toInt())
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
                    unscaledBI = BigInteger.valueOf(unscaledInt)
                }
                idx++
                d = number.digits[idx]
                if (unscaledBI == null) {
                    unscaledInt *= 10000
                    unscaledInt += d.toLong()
                } else {
                    unscaledBI = unscaledBI.multiply(BI_TEN_THOUSAND)
                    if (d.toInt() != 0) {
                        unscaledBI = unscaledBI.add(BigInteger.valueOf(d.toLong()))
                    }
                }
            }
            //now we need BigInteger to create BigDecimal
            if (unscaledBI == null) {
                unscaledBI = BigInteger.valueOf(unscaledInt)
            }
            if (sign == SIGN_NEGATIVE) {
                unscaledBI = unscaledBI!!.negate()
            }
            //the difference between len and weight (adjusted from 0 based) becomes the scale for BigDecimal
            val bigDecScale = (number.digits.size - (weight + 1)) * 4
            //string representation always results in a BigDecimal with scale of 0
            //the binary representation, where weight and len can infer trailing 0s, can result in a negative scale
            //to produce a consistent BigDecimal, we return the equivalent object with scale set to 0
            return if (bigDecScale == 0) BigDecimal(unscaledBI) else BigDecimal(
                unscaledBI,
                bigDecScale
            ).setScale(0)
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
                unscaledBI = BigInteger.valueOf(unscaledInt)
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
                    unscaledBI = unscaledBI.add(BigInteger.valueOf(d.toLong()))
                }
            }
        }

        //now we need BigInteger to create BigDecimal
        if (unscaledBI == null) {
            unscaledBI = BigInteger.valueOf(unscaledInt)
        }
        //if there is remaining weight, apply it here
        if (effectiveWeight > 0) {
            unscaledBI = unscaledBI!!.multiply(tenPower(effectiveWeight * 4))
        }
        //if there is remaining effective scale, apply it here
        if (effectiveScale > 0) {
            unscaledBI = unscaledBI!!.multiply(tenPower(effectiveScale))
        }
        if (sign == SIGN_NEGATIVE) {
            unscaledBI = unscaledBI!!.negate()
        }

        return BigDecimal(unscaledBI, scale.toInt())
    }

    companion object {
        private const val NUMERIC_DSCALE_MASK = 0x00003FFF
        private val INT_TEN_POWERS = IntArray(6) { 10.0.pow(it).toInt() }
        private val BI_TEN_POWERS = Array(32) { BigInteger.TEN.pow(it) }
        private val BI_MAX_LONG = BigInteger.valueOf(Long.MAX_VALUE)
        private val BI_TEN_THOUSAND = BigInteger.valueOf(10000)

        private fun tenPower(exponent: Int): BigInteger {
            return BI_TEN_POWERS.getOrElse(exponent) { BigInteger.TEN.pow(exponent) }
        }

        // https://github.com/postgres/postgres/blob/a6c21887a9f0251fa2331ea3ad0dd20b31c4d11d/src/backend/utils/adt/numeric.c#L1153
        internal fun fromBytes(buffer: ReadBuffer): PgNumeric {
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
            var unscaled = bigDecimal.unscaledValue().abs()
            var scale = bigDecimal.scale()
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
                    unscaled = pair[0]
                    val short = pair[1].toShort()
                    if (short != 0.toShort() || shorts.isNotEmpty()) {
                        shorts.add(short)
                    }
                    ++weight
                }
                var unscaledLong = unscaled.longValueExact()
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

            val split = unscaled.divideAndRemainder(tenPower(scale))
            var decimal = split[1]
            var wholes = split[0]
            weight = -1
            if (!BigInteger.ZERO.equals(decimal)) {
                val mod = scale % 4
                var segments = scale / 4
                if (mod != 0) {
                    decimal *= tenPower(4 - mod)
                    ++segments
                }
                do {
                    val pair = decimal.divideAndRemainder(BI_TEN_THOUSAND)
                    decimal = pair[0]
                    val short = pair[1].toShort()
                    if (short != 0.toShort() || shorts.isNotEmpty()) {
                        shorts.add(short)
                    }
                    --segments
                } while (!BigInteger.ZERO.equals(decimal))

                if (BigInteger.ZERO.equals(wholes)) {
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
                wholes = pair[0]
                val short = pair[1].toShort()
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
