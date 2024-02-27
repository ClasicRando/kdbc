package com.github.clasicrando.postgresql.type

import com.github.clasicrando.common.datetime.tryFromString
import kotlinx.datetime.LocalTime
import kotlinx.datetime.UtcOffset

data class PgTimeTz(val time: LocalTime, val offset: UtcOffset) {
    override fun toString(): String {
        return "$time$offset"
    }

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

        fun fromString(value: String): PgTimeTz {
            return PgTimeTz(
                time = LocalTime.tryFromString(value).getOrThrow(),
                offset = UtcOffset.tryFromString(value).getOrThrow(),
            )
        }
    }
}
