package com.github.clasicrando.postgresql.type

import com.github.clasicrando.common.datetime.tryFromString
import kotlinx.datetime.LocalTime
import kotlinx.datetime.UtcOffset

data class PgTimeTz(val time: LocalTime, val offset: UtcOffset) {
    override fun toString(): String {
        return "$time$offset"
    }

    companion object {
        fun fromString(value: String): PgTimeTz {
            return PgTimeTz(
                time = LocalTime.tryFromString(value).getOrThrow(),
                offset = UtcOffset.tryFromString(value).getOrThrow(),
            )
        }
    }
}
