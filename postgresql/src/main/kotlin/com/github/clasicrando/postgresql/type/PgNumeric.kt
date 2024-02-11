package com.github.clasicrando.postgresql.type

import java.math.BigDecimal
import java.math.BigInteger
import kotlin.math.pow

internal const val SIGN_NAN: Short = 0xC0
internal const val MAX_RADIX: Int = 36

internal fun base10To10000(byteArray: ByteArray): Short {
    return byteArray.fold(0.toShort()) { acc, byte ->
        (acc * 10).plus(byte).toShort()
    }
}

internal fun ByteArray.chunked(chunkSize: Int): Sequence<ByteArray> = sequence {
    for (i in this@chunked.indices step chunkSize) {
        val end = (i + chunkSize).coerceAtMost(size)
        yield(sliceArray(i..<end))
    }
}

internal fun BigInteger.toBase10Bytes(): ByteArray = this.toString()
    .map { it.digitToInt().toByte() }
    .toByteArray()

internal sealed class PgNumeric {
    data object NAN: PgNumeric()
    class Number(
        val sign: PgNumericSign,
        val digits: ShortArray,
        val weight: Short,
        val scale: Short,
    ): PgNumeric()

    internal fun toBigDecimal(): BigDecimal {
        val number = when (this) {
            NAN -> error("BigDecimal does not support Nan values")
            is Number -> this
        }

        if (number.digits.isEmpty()) {
            return BigDecimal(0)
        }

        val scale = (number.digits.size - number.weight) * 4
        val cents = number.digits
            .asSequence()
            .flatMap {
                sequenceOf((it / MAX_RADIX), (it % MAX_RADIX))
            }.joinToString(separator = "") {
                it.toString().padStart(length = 2, padChar = '0')
            }

        val bigint = BigInteger(cents, MAX_RADIX)
        return BigDecimal(bigint, scale)
    }

    companion object {
        internal fun fromBigDecimal(bigDecimal: BigDecimal): PgNumeric {
            val integer = bigDecimal.unscaledValue()
            val exponent = bigDecimal.scale()
            val scale = maxOf(0, exponent).toShort()
            val sign = integer.signum()
            val base10 = integer.toBase10Bytes()
            val weight10 = base10.size - exponent
            val weight = if (weight10 <= 0) {
                weight10 / 4 - 1
            } else {
                (weight10 - 1) / 4
            }
            val offset = weight10.rem(4)
            val digitsTemp = mutableListOf<Short>()
            val firstIndices = 0..<offset
            if (base10.indices.contains(firstIndices.last)) {
                val first = base10.sliceArray(firstIndices)
                if (first.isNotEmpty()) {
                    digitsTemp.add(base10To10000(first))
                }
                val rest = base10.sliceArray(offset..<base10.size)
                rest.chunked(4).forEach {
                    val digit = base10To10000(it) * 10.0.pow(4 - it.size)
                    digitsTemp.add(digit.toInt().toShort())
                }
            } else if (offset != 0) {
                val digit = base10To10000(base10) * 10.0.pow(offset - base10.size)
                digitsTemp.add(digit.toInt().toShort())
            }
            val digits = digitsTemp.dropLastWhile { it != 0.toShort() }.toShortArray()
            return Number(
                sign = when {
                    sign >= 0 -> PgNumericSign.Positive
                    else -> PgNumericSign.Negative
                },
                scale = scale,
                weight = weight.toShort(),
                digits = digits,
            )
        }
    }

    enum class PgNumericSign(val value: Short, val multiplier: Int) {
        Positive(0x0000, 1),
        Negative(0x4000, -1),
    }
}
