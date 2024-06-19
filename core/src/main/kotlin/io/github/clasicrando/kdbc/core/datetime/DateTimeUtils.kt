package io.github.clasicrando.kdbc.core.datetime

import kotlinx.datetime.Instant
import kotlinx.datetime.LocalDate
import kotlinx.datetime.LocalDateTime
import kotlinx.datetime.LocalTime
import kotlinx.datetime.TimeZone
import kotlinx.datetime.UtcOffset
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
        parse(value)
    } catch (_: IllegalArgumentException) {
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
        val str = value.takeWhile { it != '+' && it != 'Z' }
            .trim()
            .replace(' ', 'T')
        parse(str)
    } catch (_: IllegalArgumentException) {
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
        val str = value.takeWhile { it != '+' && it != 'Z' }
            .trim()
        parse(str)
    } catch (_: IllegalArgumentException) {
        throw InvalidDateString(value, LocalTime::class)
    }
}

/**
 * Attempt to convert the string [value] provided into a [Instant]. This will replace space
 * characters with 'T' and pad the end with 'Z' if no offset is present.
 *
 * @throws InvalidDateString if the conversion fails
 */
fun Instant.Companion.tryFromString(value: String): Instant {
    return try {
        val str = value.trim()
            .replace(oldChar = ' ',newChar = 'T')
            .padEnd(length = 20, padChar = 'Z')
        parse(str)
    } catch (_: IllegalArgumentException) {
        throw InvalidDateString(value, Instant::class)
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
        .takeIf { it.isNotBlank() }
        ?: return UtcOffset(0)
    return timeZoneStr.toIntOrNull()
        ?.let { UtcOffset(hours = it) }
        ?: throw InvalidDateString(timeZoneStr, TimeZone::class)
}
