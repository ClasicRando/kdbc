package io.github.clasicrando.kdbc.core.datetime

import kotlinx.datetime.Instant
import kotlinx.datetime.LocalDate
import kotlinx.datetime.LocalDateTime
import kotlinx.datetime.LocalTime
import kotlinx.datetime.UtcOffset
import kotlinx.datetime.toInstant

/**
 * Type storing a [Instant] as well the corresponding [UtcOffset] to qualify the datetime.
 * kotlinx-datetime does not have a native type for storing an offset datetime so this is
 * supplementary to be a parallel to `timestamp with timezone`, `datetime` or other database types
 * that stored a [datetime] and the [offset].
 */
data class DateTime(val datetime: Instant, val offset: UtcOffset) {
    constructor(date: LocalDate, time: LocalTime, offset: UtcOffset) :
        this(LocalDateTime(date, time).toInstant(offset), offset)
    constructor(localDateTime: LocalDateTime, offset: UtcOffset) :
        this(localDateTime.toInstant(offset), offset)

    /**
     * Return a new [DateTime] instance with the same scalar timestamp value, but with the new
     * [offset] applied to the [datetime] and the new [offset] stored in the result.
     */
    fun withOffset(offset: UtcOffset): DateTime {
        return DateTime(datetime, offset)
    }

    override fun toString(): String {
        return buildString {
            append(datetime)
            deleteCharAt(length - 1)
            append(offset)
        }
    }

    companion object {
        /**
         * Convert the supplied string [value] to a [DateTime] instance. The only supported
         * datetime format is ISO-8601.
         */
        fun fromString(value: String): DateTime {
            return DateTime(
                datetime = Instant.tryFromString(value),
                offset = UtcOffset.tryFromString(value),
            )
        }
    }
}
