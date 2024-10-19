package io.github.clasicrando.kdbc.postgresql.type

import com.ionspin.kotlin.bignum.decimal.BigDecimal
import io.github.clasicrando.kdbc.core.buffer.ByteWriteBuffer
import io.github.clasicrando.kdbc.core.buffer.writeLengthPrefixed
import io.github.clasicrando.kdbc.core.column.checkOrColumnDecodeError
import io.github.clasicrando.kdbc.core.datetime.DateTime
import io.github.clasicrando.kdbc.postgresql.column.PgColumnDescription
import io.github.clasicrando.kdbc.postgresql.column.PgValue
import kotlinx.datetime.Instant
import kotlinx.datetime.LocalDate
import kotlin.reflect.KTypeProjection
import kotlin.reflect.full.createType

private const val zeroRangeFlags = 0x00

// https://github.com/postgres/postgres/blob/master/src/include/utils/rangetypes.h#L38-L45
private const val emptyRangeFlagMask = 0x01
private const val lowerBoundInclusiveRangeFlagMask = 0x02
private const val upperBoundInclusiveRangeFlagMask = 0x04
private const val lowerBoundInfiniteRangeFlagMask = 0x08
private const val upperBoundInfiniteRangeFlagMask = 0x10
//private const val lowerBoundNullRangeFlagMask = 0x20
//private const val upperBoundNullRangeFlagMask = 0x40
//private const val containEmptyRangeFlagMask = 0x80

private fun rangeFlagContains(flags: Int, mask: Int): Boolean {
    return (mask and flags) == mask
}

/**
 * Base implementation of a [PgTypeDescription] for [PgRange] types that map to the respective
 * range types in a postgresql database.
 *
 * [pg docs](https://www.postgresql.org/docs/16/rangetypes.html)
 */
internal abstract class BaseRangeTypeDescription<T : Any>(
    pgType: PgType,
    private val typeDescription: PgTypeDescription<T>,
) : PgTypeDescription<PgRange<T>>(
    dbType = pgType,
    kType = PgRange::class
        .createType(arguments = listOf(KTypeProjection.invariant(typeDescription.kType))),
) {
    /**
     * Writes the range flags as a single [Byte], followed by the [PgRange.lower] and [PgRange.upper]
     * if either value is not [Bound.Unbounded]. Range flags as bitmask [Int] values from for if
     * the upper/lower bounds are inclusive or infinite (i.e. unbounded).
     *
     * [pg source code](https://github.com/postgres/postgres/blob/874d817baa160ca7e68bee6ccc9fc1848c56e750/src/backend/utils/adt/rangetypes.c#L177)
     */
    final override fun encode(value: PgRange<T>, buffer: ByteWriteBuffer) {
        var flags = zeroRangeFlags

        flags = flags or when (value.lower) {
            is Bound.Excluded -> zeroRangeFlags
            is Bound.Included -> lowerBoundInclusiveRangeFlagMask
            is Bound.Unbounded -> lowerBoundInfiniteRangeFlagMask
        }

        flags = flags or when (value.upper) {
            is Bound.Excluded -> zeroRangeFlags
            is Bound.Included -> upperBoundInclusiveRangeFlagMask
            is Bound.Unbounded -> upperBoundInfiniteRangeFlagMask
        }

        buffer.writeByte(flags.toByte())

        when (value.lower) {
            is Bound.Excluded -> buffer.writeLengthPrefixed {
                typeDescription.encode(value.lower.value, buffer)
            }
            is Bound.Included -> buffer.writeLengthPrefixed {
                typeDescription.encode(value.lower.value, buffer)
            }
            is Bound.Unbounded -> {}
        }

        when (value.upper) {
            is Bound.Excluded -> buffer.writeLengthPrefixed {
                typeDescription.encode(value.upper.value, buffer)
            }
            is Bound.Included -> buffer.writeLengthPrefixed {
                typeDescription.encode(value.upper.value, buffer)
            }
            is Bound.Unbounded -> {}
        }
    }

    /**
     * Steps to decode a generic range:
     *
     * 1. Initialize the bounds as [Bound.Unbounded] since that is the default value.
     * 2. Read a Single [Byte] as the range flags. If the flags value contains the
     * [emptyRangeFlagMask] then the start and end are [Bound.Unbounded] and the decode method
     * exits, returning a [PgRange] with those bounds
     * 2. Check if the flags value contains the [lowerBoundInfiniteRangeFlagMask]. If not then use
     * the byte buffer to decode a value of [T] to install as the starting bound. With this decoded
     * value, check the flags value to see if it contains the [lowerBoundInclusiveRangeFlagMask].
     * If yes, then lower bound is [Bound.Included]. Otherwise, the lower bound is [Bound.Excluded].
     * 2. Check if the flags value contains the [upperBoundInfiniteRangeFlagMask]. If not then use
     * the byte buffer to decode a value of [T] to install as the starting bound. With this decoded
     * value, check the flags value to see if it contains the [upperBoundInclusiveRangeFlagMask].
     * If yes, then the upper bound is [Bound.Included]. Otherwise, the upper bound is
     * [Bound.Excluded].
     *
     * [pg source code](https://github.com/postgres/postgres/blob/874d817baa160ca7e68bee6ccc9fc1848c56e750/src/backend/utils/adt/rangetypes.c#L261)
     */
    final override fun decodeBytes(value: PgValue.Binary): PgRange<T> {
        var start: Bound<T> = Bound.Unbounded()
        var end: Bound<T> = Bound.Unbounded()

        val flags = value.bytes.readByte().toInt()

        if (rangeFlagContains(flags, emptyRangeFlagMask)) {
            return PgRange(start, end)
        }

        if (!rangeFlagContains(flags, lowerBoundInfiniteRangeFlagMask)) {
            val lowerBoundValueLength = value.bytes.readInt()
            val lowerBoundPgValue = PgValue.Binary(
                bytes = value.bytes.slice(lowerBoundValueLength),
                typeData = PgColumnDescription.dummyDescription(typeDescription.dbType, 1)
            )

            val lowerBoundValue = typeDescription.decodeBytes(lowerBoundPgValue)
            start = if (rangeFlagContains(flags, lowerBoundInclusiveRangeFlagMask)) {
                Bound.Included(lowerBoundValue)
            } else {
                Bound.Excluded(lowerBoundValue)
            }
        }

        if (!rangeFlagContains(flags, upperBoundInfiniteRangeFlagMask)) {
            val upperBoundValueLength = value.bytes.readInt()
            val upperBoundPgValue = PgValue.Binary(
                bytes = value.bytes.slice(upperBoundValueLength),
                typeData = PgColumnDescription.dummyDescription(typeDescription.dbType, 1)
            )

            val upperBoundValue = typeDescription.decodeBytes(upperBoundPgValue)
            end = if (rangeFlagContains(flags, upperBoundInclusiveRangeFlagMask)) {
                Bound.Included(upperBoundValue)
            } else {
                Bound.Excluded(upperBoundValue)
            }
        }

        return PgRange(lower = start, upper = end)
    }

    private fun decodeBound(char: Char, value: T): Bound<T> {
        return when (char) {
            '(', ')' -> Bound.Excluded(value)
            '[', ']' -> Bound.Included(value)
            else -> error("Expected bound character but found '$char'")
        }
    }

    /**
     * Strip the bound characters from the [String] value and provide that resulting value to
     * [PgRangeLiteralParser.parse] to get the start and end values of range. When actual [String]
     * values are available for either bound, pass to the inner [typeDescription] to decode and
     * interpret as an inclusive or inclusive bound. If either range value is empty/null, default
     * to [Bound.Unbounded]. After the 2 bounds have been decoded, combine into a new [PgRange]
     * instance.
     *
     * @throws io.github.clasicrando.kdbc.core.column.ColumnDecodeError if the number of bounds in
     * the range literal is > 2 or the inner [typeDescription] throws an error
     */
    //https://github.com/postgres/postgres/blob/874d817baa160ca7e68bee6ccc9fc1848c56e750/src/backend/utils/adt/rangetypes.c#L137
    final override fun decodeText(value: PgValue.Text): PgRange<T> {
        val lower = value.text.first()
        val upper = value.text.last()

        val slice = value.text.substring(1, value.text.length - 1)

        val bounds = PgRangeLiteralParser.parse(slice).toList()
        checkOrColumnDecodeError(
            check = bounds.size <= 2,
            kType = kType,
            type = value.typeData,
        ) { "Cannot parse range literal with more than 2 elements" }

        val start = bounds.getOrNull(0)
            ?.let {
                val lowerBoundValue = typeDescription.decodeText(PgValue.Text(it, value.typeData))
                decodeBound(lower, lowerBoundValue)
            }
            ?: Bound.Unbounded()
        val end = bounds.getOrNull(1)
            ?.let {
                val upperBoundValue = typeDescription.decodeText(PgValue.Text(it, value.typeData))
                decodeBound(upper, upperBoundValue)
            }
            ?: Bound.Unbounded()
        return PgRange(lower = start, upper = end)
    }
}

typealias Int8Range = PgRange<Long>

/**
 * Implementation of a [PgTypeDescription] for the [Int8Range] type. This maps to the `int8range`
 * type in a postgresql database.
 */
internal object Int8RangeTypeDescription : BaseRangeTypeDescription<Long>(
    pgType = PgType.Int8Range,
    typeDescription = BigIntTypeDescription,
)

typealias Int4Range = PgRange<Int>

/**
 * Implementation of a [PgTypeDescription] for the [Int4Range] type. This maps to the `int4range`
 * type in a postgresql database.
 */
internal object Int4RangeTypeDescription : BaseRangeTypeDescription<Int>(
    pgType = PgType.Int4Range,
    typeDescription = IntTypeDescription,
)

typealias TsRange = PgRange<Instant>

/**
 * Implementation of a [PgTypeDescription] for the [TsRange] type. This maps to the `tsrange`
 * type in a postgresql database.
 */
internal object TsRangeTypeDescription : BaseRangeTypeDescription<Instant>(
    pgType = PgType.TsRange,
    typeDescription = InstantTypeDescription,
)

typealias JTsRange = PgRange<java.time.LocalDateTime>

/**
 * Implementation of a [PgTypeDescription] for the [JTsRange] type. This maps to the `tsrange`
 * type in a postgresql database.
 */
internal object JTsRangeTypeDescription : BaseRangeTypeDescription<java.time.LocalDateTime>(
    pgType = PgType.TsRange,
    typeDescription = LocalDateTimeTypeDescription,
)

typealias TsTzRange = PgRange<DateTime>

/**
 * Implementation of a [PgTypeDescription] for the [TsTzRange] type. This maps to the `tstzrange`
 * type in a postgresql database.
 */
internal object TsTzRangeTypeDescription : BaseRangeTypeDescription<DateTime>(
    pgType = PgType.TstzRange,
    typeDescription = DateTimeTypeDescription,
)

typealias JTsTzRange = PgRange<java.time.OffsetDateTime>

/**
 * Implementation of a [PgTypeDescription] for the [JTsTzRange] type. This maps to the `tstzrange`
 * type in a postgresql database.
 */
internal object JTsTzRangeTypeDescription : BaseRangeTypeDescription<java.time.OffsetDateTime>(
    pgType = PgType.TstzRange,
    typeDescription = OffsetDateTimeTypeDescription,
)

typealias DateRange = PgRange<LocalDate>

/**
 * Implementation of a [PgTypeDescription] for the [DateRange] type. This maps to the `daterange`
 * type in a postgresql database.
 */
internal object DateRangeTypeDescription : BaseRangeTypeDescription<LocalDate>(
    pgType = PgType.DateRange,
    typeDescription = LocalDateTypeDescription,
)

typealias JDateRange = PgRange<java.time.LocalDate>

/**
 * Implementation of a [PgTypeDescription] for the [JDateRange] type. This maps to the `daterange`
 * type in a postgresql database.
 */
internal object JDateRangeTypeDescription : BaseRangeTypeDescription<java.time.LocalDate>(
    pgType = PgType.DateRange,
    typeDescription = JLocalDateTypeDescription,
)

typealias NumRange = PgRange<BigDecimal>

/**
 * Implementation of a [PgTypeDescription] for the [NumRange] type. This maps to the `numrange`
 * type in a postgresql database.
 */
internal object NumRangeTypeDescription : BaseRangeTypeDescription<BigDecimal>(
    pgType = PgType.NumRange,
    typeDescription = BigDecimalTypeDescription,
)
