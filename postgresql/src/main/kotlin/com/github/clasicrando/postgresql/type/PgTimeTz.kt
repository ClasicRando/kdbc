package com.github.clasicrando.postgresql.type

import com.github.clasicrando.common.datetime.tryFromString
import kotlinx.datetime.LocalTime
import kotlinx.datetime.TimeZone

data class PgTimeTz(val time: LocalTime, val timeZone: TimeZone) {
    override fun toString(): String {
        return "$time$timeZone"
    }

    companion object {
        fun fromString(value: String): PgTimeTz {
            return PgTimeTz(
                time = LocalTime.tryFromString(value).getOrThrow(),
                timeZone = TimeZone.tryFromString(value).getOrThrow(),
            )
        }
    }
}
