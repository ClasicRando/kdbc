package io.github.clasicrando.kdbc.core.datetime

import kotlinx.datetime.Instant
import kotlinx.datetime.LocalDate
import kotlinx.datetime.LocalDateTime
import kotlinx.datetime.LocalTime
import org.junit.jupiter.api.assertDoesNotThrow
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource

class TestDateTimeUtils {
    @ParameterizedTest
    @ValueSource(strings = ["2023-01-01", "2020-05-19"])
    fun `LocalDate_tryFromString should return success when valid iso-8601 string`(value: String) {
        assertDoesNotThrow { LocalDate.tryFromString(value) }
    }

    @ParameterizedTest
    @ValueSource(strings = ["Test", "2023-01-01T08:09:57", "2020-05-19 06:59:19"])
    fun `LocalDate_tryFromString should return failure when invalid string`(value: String) {
        assertThrows<InvalidDateString> { LocalDate.tryFromString(value) }
    }

    @ParameterizedTest
    @ValueSource(strings = [
        "2020-05-19 06:59:19",
        "2020-05-19T06:59:19",
        "2020-05-19 06:59:19+08",
        "2023-01-01T23:56:45Z",
        "2023-01-01 23:56:45Z",
    ])
    fun `LocalDateTime_tryFromString should return success when valid iso-8601 string`(value: String) {
        assertDoesNotThrow { LocalDateTime.tryFromString(value) }
    }

    @ParameterizedTest
    @ValueSource(strings = [
        "Test",
        "2023-01-01T08:09:57-08",
        "2020-05-1906:59:19",
        "2023-01-01Z08:09:57-08",
    ])
    fun `LocalDateTime_tryFromString should return failure when invalid string`(value: String) {
        assertThrows<InvalidDateString> { LocalDateTime.tryFromString(value) }
    }

    @ParameterizedTest
    @ValueSource(strings = [
        "06:59:19",
        "23:56:45",
    ])
    fun `LocalTime_tryFromString should return success when valid iso-8601 string`(value: String) {
        assertDoesNotThrow { LocalTime.tryFromString(value) }
    }

    @ParameterizedTest
    @ValueSource(strings = [
        "Test",
        "08:09:57-08",
    ])
    fun `LocalTime_tryFromString should return failure when invalid string`(value: String) {
        assertThrows<InvalidDateString> { LocalTime.tryFromString(value) }
    }

    @ParameterizedTest
    @ValueSource(strings = [
        "2020-05-19T06:59:19-03",
        "2020-05-19T06:59:19+09",
        "2020-05-19 06:59:19",
        "2023-01-01T23:56:45Z",
        "2023-01-01T23:56:45Z",
    ])
    fun `Instant_tryFromString should return success when valid iso-8601 string`(value: String) {
        assertDoesNotThrow { Instant.tryFromString(value) }
    }

    @ParameterizedTest
    @ValueSource(strings = [
        "Test",
        "2020-05-1906:59:19",
        "2023-01-01Z08:09:57-08",
    ])
    fun `Instant_tryFromString should return failure when invalid string`(value: String) {
        assertThrows<InvalidDateString> { Instant.tryFromString(value) }
    }
}
