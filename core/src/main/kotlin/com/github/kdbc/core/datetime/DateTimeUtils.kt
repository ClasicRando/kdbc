package com.github.kdbc.core.datetime

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
 * Attempt to convert the string [value] provided into a [LocalDate].
 *
 * @throws InvalidDateString if the conversion fails
 */
fun LocalDate.Companion.tryFromString(value: String): LocalDate {
    return try {
        value.toLocalDate()
    } catch (ex: IllegalArgumentException) {
        throw InvalidDateString(value, LocalDate::class)
    }
}

/**
 * Attempt to convert the string [value] provided into a [LocalDateTime]
 *
 * @throws InvalidDateString if the conversion fails
 */
fun LocalDateTime.Companion.tryFromString(value: String): LocalDateTime {
    return try {
        value.takeWhile { it != '+' && it != 'Z' }
            .trim()
            .replace(' ', 'T')
            .toLocalDateTime()
    } catch (ex: IllegalArgumentException) {
        throw InvalidDateString(value, LocalDateTime::class)
    }
}

/**
 * Attempt to convert the string [value] provided into a [LocalTime]
 *
 * @throws InvalidDateString if the conversion fails
 */
fun LocalTime.Companion.tryFromString(value: String): LocalTime {
    return try {
        value.takeWhile { it != '+' && it != 'Z' }
            .trim()
            .toLocalTime()
    } catch (ex: IllegalArgumentException) {
        throw InvalidDateString(value, LocalTime::class)
    }
}

/**
 * Attempt to extract a [TimeZone] from the date string [value] provided. If the extraction fails,
 * an [InvalidDateString] exception will be returns within the [Result]. If there is no timezone
 * information in the string, [TimeZone.UTC] is returned.
 *
 * @throws InvalidDateString if the conversion fails
 */
fun UtcOffset.Companion.tryFromString(value: String): UtcOffset {
    val timeZoneStr = value.dropWhile { it != '+' }
        .takeIf { it.isNotBlank() } ?: return UtcOffset(0)
    return timeZoneStr.toIntOrNull()
        ?.let { UtcOffset(hours = it) }
        ?: throw InvalidDateString(timeZoneStr, TimeZone::class)
}
