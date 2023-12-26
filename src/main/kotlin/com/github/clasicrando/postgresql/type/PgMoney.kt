package com.github.clasicrando.postgresql.type

import kotlinx.serialization.KSerializer
import kotlinx.serialization.Serializable
import kotlinx.serialization.descriptors.PrimitiveKind
import kotlinx.serialization.descriptors.PrimitiveSerialDescriptor
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.encoding.Decoder
import kotlinx.serialization.encoding.Encoder
import kotlin.math.absoluteValue

@Serializable(with = PgMoney.Companion::class)
class PgMoney private constructor(internal val integer: Long) {
    constructor(strMoney: String): this(integerFromString(strMoney))

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

    override fun toString(): String = strRep

    companion object : KSerializer<PgMoney> {
        private val MONEY_REGEX = Regex("^-?\\$?\\d+(.\\d{1,2})?$")

        override val descriptor: SerialDescriptor = PrimitiveSerialDescriptor(
            "PgMoney",
            PrimitiveKind.STRING,
        )

        override fun deserialize(decoder: Decoder): PgMoney = PgMoney(decoder.decodeString())

        override fun serialize(encoder: Encoder, value: PgMoney) {
            encoder.encodeString(value.strRep)
        }

        private fun integerFromString(strMoney: String): Long {
            require(strMoney.matches(MONEY_REGEX)) {
                """
            String supplied to PgMoney does not match the required pattern
            Pattern: '${MONEY_REGEX.pattern}'
            Actual Value: $strMoney
            """.trimIndent()
            }
            return strMoney.filter(predicate = Char::isDigit)
                .toLong() * if (strMoney.startsWith('-')) -1 else 1
        }
    }
}
