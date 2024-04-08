package io.github.clasicrando.kdbc.postgresql.type

import io.github.clasicrando.kdbc.core.datetime.InvalidDateString
import io.github.clasicrando.kdbc.core.datetime.tryFromString
import kotlinx.datetime.LocalTime
import kotlinx.datetime.UtcOffset

/**
 * Postgresql specific `time with time zone` type. Stores the [time] and timezone [offset] of the
 * value.
 */
data class PgTimeTz(val time: LocalTime, val offset: UtcOffset) {
    override fun toString(): String {
        return "$time$offset"
    }

    /**
     * If the other value is [PgTimeTz] then the total nanoseconds from the start of day (time zone
     * adjusted) is compared for the [equals] method. This means 2 times that are functionally
     * equivalent but in different time zones with return true. For example,
     * PgTimeTz('19:14:45+05') == PgTimeTz('14:14:45+00').
     */
    override fun equals(other: Any?): Boolean {
        if (other !is PgTimeTz) {
            return false
        }
        val otherNanoSeconds = other.time.toNanosecondOfDay() +
                other.offset.totalSeconds * NANOSECONDS_TO_SECONDS
        val thisNanoSeconds = this.time.toNanosecondOfDay() +
                this.offset.totalSeconds * NANOSECONDS_TO_SECONDS
        return otherNanoSeconds == thisNanoSeconds
    }

    override fun hashCode(): Int {
        var result = time.hashCode()
        result = 31 * result + offset.hashCode()
        return result
    }

    companion object {
        private const val NANOSECONDS_TO_SECONDS = 10_000_000_000L

        /**
         * Convert the [value] into the [LocalTime] and [UtcOffset] components that make up a new
         * [PgTimeTz].
         *
         * @throws InvalidDateString
         */
        fun fromString(value: String): PgTimeTz {
            return PgTimeTz(
                time = LocalTime.tryFromString(value),
                offset = UtcOffset.tryFromString(value),
            )
        }
    }
}
