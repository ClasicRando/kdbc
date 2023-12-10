package com.github.clasicrando.common.datetime

import kotlinx.datetime.LocalDate
import kotlinx.datetime.LocalDateTime
import kotlinx.datetime.LocalTime
import kotlinx.datetime.TimeZone
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class TestDateTimeUtils {
    @ParameterizedTest
    @ValueSource(strings = ["2023-01-01", "2020-05-19"])
    fun `LocalDate_tryFromString should return success when valid iso-8601 string`(value: String) {
        val result = LocalDate.tryFromString(value)

        assertTrue(result.isSuccess, "Result = $result")
        assertEquals(value, result.getOrThrow().toString())
    }

    @ParameterizedTest
    @ValueSource(strings = ["Test", "2023-01-01T08:09:57", "2020-05-19 06:59:19"])
    fun `LocalDate_tryFromString should return failure when invalid string`(value: String) {
        val result = LocalDate.tryFromString(value)

        assertTrue(result.isFailure, "Result = $result")
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
        val result = LocalDateTime.tryFromString(value)

        assertTrue(result.isSuccess, "Result = $result")
    }

    @ParameterizedTest
    @ValueSource(strings = [
        "Test",
        "2023-01-01T08:09:57-08",
        "2020-05-1906:59:19",
        "2023-01-01Z08:09:57-08",
    ])
    fun `LocalDateTime_tryFromString should return failure when invalid string`(value: String) {
        val result = LocalDateTime.tryFromString(value)

        assertTrue(result.isFailure, "Result = $result")
    }

    @ParameterizedTest
    @ValueSource(strings = [
        "06:59:19",
        "23:56:45",
    ])
    fun `LocalTime_tryFromString should return success when valid iso-8601 string`(value: String) {
        val result = LocalTime.tryFromString(value)

        assertTrue(result.isSuccess, "Result = $result")
    }

    @ParameterizedTest
    @ValueSource(strings = [
        "Test",
        "08:09:57-08",
    ])
    fun `LocalTime_tryFromString should return failure when invalid string`(value: String) {
        val result = LocalTime.tryFromString(value)

        assertTrue(result.isFailure, "Result = $result")
    }

    @ParameterizedTest
    @ValueSource(strings = [
        "06:59:19+08",
        "23:56:45+04",
        "+08",
        "+04",
    ])
    fun `TimeZone_tryFromString should return success when valid timezone`(value: String) {
        val result = TimeZone.tryFromString(value)

        assertTrue(result.isSuccess, "Result = $result")
    }

    @ParameterizedTest
    @ValueSource(strings = [
        "Test",
        "08:09:57-08",
    ])
    fun `TimeZone_tryFromString should return UTC when no timezone present`(value: String) {
        val result = TimeZone.tryFromString(value)

        assertTrue(result.isSuccess, "Result = $result")
        assertEquals(TimeZone.UTC, result.getOrThrow())
    }

    @ParameterizedTest
    @ValueSource(strings = [
        "Test+Test",
        "08:09:57+99",
    ])
    fun `TimeZone_tryFromString should return failure when invalid string`(value: String) {
        val result = TimeZone.tryFromString(value)

        assertTrue(result.isFailure, "Result = $result")
    }
}
