package com.github.clasicrando.postgresql.type

import kotlinx.serialization.KSerializer
import kotlinx.serialization.Serializable
import kotlinx.serialization.descriptors.PrimitiveKind
import kotlinx.serialization.descriptors.PrimitiveSerialDescriptor
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.encoding.Decoder
import kotlinx.serialization.encoding.Encoder
import java.math.BigDecimal
import java.math.RoundingMode
import kotlin.math.absoluteValue

@Serializable(with = PgMoney.Companion::class)
class PgMoney internal constructor(internal val integer: Long) {
    constructor(double: Double): this(BigDecimal.valueOf(double))
    constructor(decimal: BigDecimal): this(
        decimal.setScale(2, RoundingMode.UP).unscaledValue().toLong()
    ) {
        require(decimal.scale() <= 2) {
            "Money values cannot be constructed from Double values with more than 2 values after" +
                    " the decimal place. Otherwise, precision would be lost"
        }
    }

    private val strRep: String by lazy {
        buildString {
            if (this@PgMoney.integer < 0) {
                append('-')
            }
            append('$')
            val chars = this@PgMoney.integer.absoluteValue.toString()
            if (chars.length > 2) {
                for (char in chars.asSequence().take(chars.length - 2)) {
                    append(char)
                }
            } else {
                append('0')
            }
            append('.')
            when {
                chars.length >= 2 -> {
                    append(chars[chars.length - 2])
                    append(chars[chars.length - 1])
                }
                chars.length == 1 -> {
                    append('0')
                    append(chars[0])
                }
                else -> {
                    append('0')
                    append('0')
                }
            }
        }
    }

    operator fun plus(other: PgMoney): PgMoney {
        return PgMoney(this.integer + other.integer)
    }

    operator fun minus(other: PgMoney): PgMoney {
        return PgMoney(this.integer - other.integer)
    }

    override fun equals(other: Any?): Boolean {
        if (other !is PgMoney) {
            return false
        }
        return other.integer == this.integer
    }

    override fun toString(): String = strRep
    override fun hashCode(): Int {
        return integer.hashCode()
    }

    companion object : KSerializer<PgMoney> {
        private val MONEY_REGEX = Regex("^-?\\$?\\d+(.\\d{1,2})?$")

        override val descriptor: SerialDescriptor = PrimitiveSerialDescriptor(
            "PgMoney",
            PrimitiveKind.STRING,
        )

        override fun deserialize(decoder: Decoder): PgMoney = fromString(decoder.decodeString())

        override fun serialize(encoder: Encoder, value: PgMoney) {
            encoder.encodeString(value.strRep)
        }

        /**
         * Convert the [String] provided to a new instance of [PgMoney]. This is done by first
         * converting the [String] to a [BigDecimal] that provides a [Long] value after scaling the
         * decimal correctly.
         *
         * @throws IllegalArgumentException if the [String] value provided does not match the money
         * regex defined as '^-?\$?\d+(.\d{1,2})?$'
         */
        fun fromString(strMoney: String): PgMoney {
            require(strMoney.matches(MONEY_REGEX)) {
                """
                String supplied to PgMoney does not match the required pattern
                Pattern: '${MONEY_REGEX.pattern}'
                Actual Value: $strMoney
                """.trimIndent()
            }
            val long = BigDecimal(strMoney.replace("$", ""))
            return PgMoney(long)
        }
    }
}
