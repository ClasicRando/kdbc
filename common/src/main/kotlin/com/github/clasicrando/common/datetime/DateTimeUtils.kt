package com.github.clasicrando.common.datetime

import com.github.clasicrando.common.mapError
import kotlinx.datetime.LocalDate
import kotlinx.datetime.LocalDateTime
import kotlinx.datetime.LocalTime
import kotlinx.datetime.TimeZone
import kotlinx.datetime.UtcOffset
import kotlinx.datetime.toLocalDate
import kotlinx.datetime.toLocalDateTime
import kotlinx.datetime.toLocalTime
import kotlin.reflect.KClass

/** Exception thrown when a string cannot be converted to the required date type */
class InvalidDateString(
    value: String,
    cls: KClass<*>,
) : Throwable("Cannot parse date string, '$value', into $cls")

/**
 * Attempt to convert the string [value] provided into a [LocalDate]. If the conversion fails, an
 * [InvalidDateString] exception will be returns within the [Result].
 */
fun LocalDate.Companion.tryFromString(value: String): Result<LocalDate> {
    return kotlin.runCatching {
        value.toLocalDate()
    }.mapError { t ->
        when (t) {
            is IllegalArgumentException -> InvalidDateString(value, LocalDate::class)
            else -> t
        }
    }
}

/**
 * Attempt to convert the string [value] provided into a [LocalDateTime]. If the conversion fails,
 * an [InvalidDateString] exception will be returns within the [Result].
 */
fun LocalDateTime.Companion.tryFromString(value: String): Result<LocalDateTime> {
    return kotlin.runCatching {
        value.takeWhile { it != '+' && it != 'Z' }
            .trim()
            .replace(' ', 'T')
            .toLocalDateTime()
    }.mapError { t ->
        when (t) {
            is IllegalArgumentException -> InvalidDateString(value, LocalDateTime::class)
            else -> t
        }
    }
}

/**
 * Attempt to convert the string [value] provided into a [LocalTime]. If the conversion fails, an
 * [InvalidDateString] exception will be returns within the [Result].
 */
fun LocalTime.Companion.tryFromString(value: String): Result<LocalTime> {
    return kotlin.runCatching {
        value.toLocalTime()
    }.mapError { t ->
        when (t) {
            is IllegalArgumentException -> InvalidDateString(value, LocalTime::class)
            else -> t
        }
    }
}

/**
 * Attempt to extract a [TimeZone] from the date string [value] provided. If the extraction fails,
 * an [InvalidDateString] exception will be returns within the [Result]. If there is no timezone
 * information in the string, [TimeZone.UTC] is returned.
 */
fun UtcOffset.Companion.tryFromString(value: String): Result<UtcOffset> {
    return kotlin.runCatching {
        val timeZoneStr = value.dropWhile { it != '+' }
            .takeIf { it.isNotBlank() } ?: return@runCatching UtcOffset(0)
        timeZoneStr.toIntOrNull()
            ?.let { UtcOffset(hours = it) }
            ?: throw InvalidDateString(timeZoneStr, TimeZone::class)
    }
}
