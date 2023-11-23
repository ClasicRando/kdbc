package com.github.clasicrando.postgresql.type

import kotlinx.serialization.Serializable
import kotlin.math.absoluteValue

@Serializable
data class PgMoney(val integer: Int) {
    private val strRep: String get() = buildString {
        if (integer < 0) {
            append('-')
        }
        val chars = integer.absoluteValue.toString()
        if (chars.length > 2) {
            append(chars.dropLast(2))
        } else {
            append('0')
        }
        append('.')
        append(chars.takeLast(2).padStart(2, '0'))
    }

    operator fun plus(other: PgMoney): PgMoney {
        return PgMoney(this.integer + other.integer)
    }

    operator fun minus(other: PgMoney): PgMoney {
        return PgMoney(this.integer - other.integer)
    }

    override fun toString(): String = strRep

    companion object {
        private val MONEY_REGEX = Regex("^-?$?\\d+(.\\d{1,2})?$")

        fun fromString(money: String): PgMoney {
            require(money.matches(MONEY_REGEX)) {
                """
                String supplied to PgMoney does not match the required pattern
                Pattern: '${MONEY_REGEX.pattern}'
                Actual Value: $money
                """.trimIndent()
            }
            return PgMoney(
                integer = money.filter(predicate = Char::isDigit)
                    .toInt() * if (money.startsWith('-')) -1 else 1
            )
        }
    }
}
