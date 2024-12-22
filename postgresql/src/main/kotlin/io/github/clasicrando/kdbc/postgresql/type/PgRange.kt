package io.github.clasicrando.kdbc.postgresql.type

import java.math.BigDecimal
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.OffsetDateTime
import java.time.temporal.ChronoUnit
import kotlin.reflect.KType
import kotlin.reflect.typeOf

/**
 * Postgresql range bound for [PgRange]. Bounds can include the specified value, exclude the
 * specified value or be unbounded (aka infinite).
 */
public sealed interface Bound<T> {
    public data class Included<T>(val value: T) : Bound<T>

    public data class Excluded<T>(val value: T) : Bound<T>

    public class Unbounded<T>(private val kType: KType) : Bound<T> {
        override fun equals(other: Any?): Boolean {
            return other is Unbounded<*> && this.kType == other.kType
        }

        override fun hashCode(): Int {
            return this::class.hashCode()
        }

        override fun toString(): String {
            return "Unbounded<$kType>"
        }
    }

    public companion object {
        public inline fun <reified T : Any> unbounded(): Unbounded<T> {
            return Unbounded(typeOf<T>())
        }
    }
}

/**
 * Class for Postgresql `range` types that can be generic over a comparable type. Each instance of
 * the range has a [lower] and [upper] [Bound].
 */
public data class PgRange<T : Any>(val lower: Bound<T>, val upper: Bound<T>) {
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

/** Type alias to represent a postgresql `int8range` */
public typealias Int8Range = PgRange<Long>

/** Type alias to represent a postgresql `int4range` */
public typealias Int4Range = PgRange<Int>

/** Type alias to represent a postgresql `tsrange` */
public typealias TsRange = PgRange<LocalDateTime>

/** Type alias to represent a postgresql `tstzrange` */
public typealias TsTzRange = PgRange<OffsetDateTime>

/** Type alias to represent a postgresql `daterange` */
public typealias DateRange = PgRange<LocalDate>

/** Type alias to represent a postgresql `numrange` */
public typealias NumRange = PgRange<BigDecimal>

/** Convert an [Int8Range] to a standard [IntRange] */
public fun Int4Range.toIntRange(): IntRange? {
    val start =
        when (this.lower) {
            is Bound.Excluded -> lower.value + 1
            is Bound.Included -> lower.value
            is Bound.Unbounded -> return null
        }
    val endInclusive =
        when (this.upper) {
            is Bound.Excluded -> upper.value - 1
            is Bound.Included -> upper.value
            is Bound.Unbounded -> return null
        }
    return IntRange(start, endInclusive)
}

/** Convert an [Int8Range] to a standard [LongRange] */
public fun Int8Range.toLongRange(): LongRange? {
    val start =
        when (this.lower) {
            is Bound.Excluded -> lower.value + 1
            is Bound.Included -> lower.value
            is Bound.Unbounded -> return null
        }
    val endInclusive =
        when (this.upper) {
            is Bound.Excluded -> upper.value - 1
            is Bound.Included -> upper.value
            is Bound.Unbounded -> return null
        }
    return LongRange(start, endInclusive)
}

/**
 * Convert a [DateRange] to a standard [ClosedRange] of [LocalDate]. Returns null when either bound
 * is [Bound.Unbounded] since that cannot be represented by a [ClosedRange]
 */
public fun DateRange.toDateRange(): ClosedRange<LocalDate>? {
    val startDate =
        when (this.lower) {
            is Bound.Excluded -> lower.value.plus(1, ChronoUnit.DAYS)
            is Bound.Included -> lower.value
            is Bound.Unbounded -> return null
        }
    val endDateInclusive =
        when (this.upper) {
            is Bound.Excluded -> upper.value.minus(1, ChronoUnit.DAYS)
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
