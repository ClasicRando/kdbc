package com.github.clasicrando.common.datetime

import kotlinx.datetime.LocalDateTime
import kotlinx.datetime.TimeZone

/**
 * Type storing a [LocalDateTime] as well the corresponding [TimeZone] to clarify the UTC offset of
 * the datetime. kotlinx-datetime does not have a native type for storing an offset datetime so
 * this is supplementary to be a parallel to `timestamp with timezone`, `datetime` or other
 * database types that store a [datetime] and the [timeZone] offset.
 */
data class DateTime(val datetime: LocalDateTime, val timeZone: TimeZone) {
    override fun toString(): String {
        return "$datetime$timeZone"
    }

    companion object {
        /**
         * Convert the supplied string [value] to be converted to a [DateTime] instance. The only
         * supported datetime format is ISO-8601.
         */
        fun fromString(value: String): DateTime {
            return DateTime(
                datetime = LocalDateTime.tryFromString(value).getOrThrow(),
                timeZone = TimeZone.tryFromString(value).getOrThrow(),
            )
        }
    }
}
