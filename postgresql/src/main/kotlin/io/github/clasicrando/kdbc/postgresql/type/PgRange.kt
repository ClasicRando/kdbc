package io.github.clasicrando.kdbc.postgresql.type

import kotlinx.datetime.DateTimeUnit
import kotlinx.datetime.LocalDate
import kotlinx.datetime.minus
import kotlinx.datetime.plus

sealed interface Bound<T> {
    data class Included<T>(val value: T) : Bound<T>
    data class Excluded<T>(val value: T): Bound<T>
    class Unbounded<T> : Bound<T> {
        override fun equals(other: Any?): Boolean {
            return other is Unbounded<*>
        }

        override fun hashCode(): Int {
            return this::class.hashCode()
        }

        override fun toString(): String {
            return "Unbounded"
        }
    }
}

data class PgRange<T : Any>(
    val lower: Bound<T>,
    val upper: Bound<T>,
) {
    val postgresqlLiteral: String by lazy {
        buildString {
            when (lower) {
                is Bound.Excluded -> {
                    append('(')
                    append(lower.value)
                }
                is Bound.Included -> {
                    append('[')
                    append(lower.value)
                }
                is Bound.Unbounded -> append('(')
            }
            append(',')
            when (upper) {
                is Bound.Excluded -> {
                    append(upper.value)
                    append(')')
                }
                is Bound.Included -> {
                    append(upper.value)
                    append(']')
                }
                is Bound.Unbounded -> append(')')
            }
        }
    }
}

fun PgRange<Int>.toIntRange(): IntRange? {
    val start = when (this.lower) {
        is Bound.Excluded -> lower.value + 1
        is Bound.Included -> lower.value
        is Bound.Unbounded -> return null
    }
    val endInclusive = when (this.upper) {
        is Bound.Excluded -> upper.value - 1
        is Bound.Included -> upper.value
        is Bound.Unbounded -> return null
    }
    return IntRange(start, endInclusive)
}

fun PgRange<Long>.toLongRange(): LongRange? {
    val start = when (this.lower) {
        is Bound.Excluded -> lower.value + 1
        is Bound.Included -> lower.value
        is Bound.Unbounded -> return null
    }
    val endInclusive = when (this.upper) {
        is Bound.Excluded -> upper.value - 1
        is Bound.Included -> upper.value
        is Bound.Unbounded -> return null
    }
    return LongRange(start, endInclusive)
}

fun PgRange<LocalDate>.toDateRange(): ClosedRange<LocalDate>? {
    val startDate = when (this.lower) {
        is Bound.Excluded -> lower.value.plus(1, DateTimeUnit.DAY)
        is Bound.Included -> lower.value
        is Bound.Unbounded -> return null
    }
    val endDateInclusive = when (this.upper) {
        is Bound.Excluded -> upper.value.minus(1, DateTimeUnit.DAY)
        is Bound.Included -> upper.value
        is Bound.Unbounded -> return null
    }
    return object : ClosedRange<LocalDate> {
        override val start: LocalDate = startDate
        override val endInclusive: LocalDate = endDateInclusive

        override fun equals(other: Any?): Boolean {
            if (this === other) return true
            if (other !is ClosedRange<*>) return false
            return this.start == other.start && this.endInclusive == other.endInclusive
        }

        override fun toString(): String {
            return "$start..$endInclusive"
        }
    }
}
