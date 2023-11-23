package com.github.clasicrando.common.datetime

import com.github.clasicrando.common.mapError
import io.klogging.noCoLogger
import kotlinx.datetime.LocalDate
import kotlinx.datetime.LocalDateTime
import kotlinx.datetime.LocalTime
import kotlinx.datetime.TimeZone
import kotlinx.datetime.toLocalDateTime
import kotlinx.datetime.toLocalTime
import kotlin.reflect.KClass

val logger = noCoLogger("DateTimeUtilsKt")

class InvalidDateString(
    value: String,
    cls: KClass<*>,
) : Throwable("Cannot parse date string, '$value', into $cls")

fun LocalDate.Companion.tryFromString(str: String): Result<LocalDateTime> {
    return kotlin.runCatching {
        str.takeWhile { it != '+' && it != 'Z' }
            .trim()
            .replace(' ', 'T')
            .toLocalDateTime()
    }.mapError { t ->
        when (t) {
            is IllegalArgumentException -> InvalidDateString(str, LocalDate::class)
            else -> t
        }
    }
}

fun LocalDateTime.Companion.tryFromString(str: String): Result<LocalDateTime> {
    return kotlin.runCatching {
        str.takeWhile { it != '+' && it != 'Z' }
            .trim()
            .replace(' ', 'T')
            .toLocalDateTime()
    }.mapError { t ->
        when (t) {
            is IllegalArgumentException -> InvalidDateString(str, LocalDateTime::class)
            else -> t
        }
    }
}

fun LocalTime.Companion.tryFromString(str: String): Result<LocalTime> {
    return kotlin.runCatching {
        str.toLocalTime()
    }.mapError { t ->
        when (t) {
            is IllegalArgumentException -> InvalidDateString(str, LocalTime::class)
            else -> t
        }
    }
}

fun TimeZone.Companion.fromStringOrDefault(str: String): TimeZone {
    return kotlin.runCatching {
        str.dropWhile { it != '+' }
            .takeIf { it.isNotBlank() }
            ?.let { of(it) }
            ?: UTC
    }.onFailure {
        logger.warn(
            "Could not parse timezone value from {str}. Error suppressed and UTC returned",
            str
        )
    }.getOrDefault(UTC)
}
