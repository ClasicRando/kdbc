package com.github.clasicrando.common.datetime

import kotlinx.datetime.LocalDateTime
import kotlinx.datetime.TimeZone

data class DateTime(val datetime: LocalDateTime, val timeZone: TimeZone) {
    override fun toString(): String {
        return "$datetime$timeZone"
    }

    companion object {
        fun fromString(value: String): DateTime {
            return DateTime(
                datetime = LocalDateTime.tryFromString(value).getOrThrow(),
                timeZone = TimeZone.fromStringOrDefault(value),
            )
        }
    }
}
