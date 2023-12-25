package com.github.clasicrando.postgresql.column

import com.github.clasicrando.postgresql.type.PgMoney
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.CsvSource
import org.junit.jupiter.params.provider.ValueSource
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

class TestPgMoney {
    @ParameterizedTest
    @ValueSource(strings = [
        "This is a test",
        "2.63265E+10",
        "@22.63",
        "$22.63526",
        "-$22.63526",
        "22.",
    ])
    fun `PgMoney should fail when string does not match the money regex pattern`(
        strMoney: String,
    ) {
        assertFailsWith<IllegalArgumentException> {
            PgMoney(strMoney)
        }
    }

    @ParameterizedTest
    @CsvSource(
        "-$589.25,-58925",
        "$4152.23,415223",
        "$4152.2,41522",
        "85.96,8596",
        "85.9,859",
        "85,85",
    )
    fun `PgMoney should construct new instance when string matches the money regex pattern`(
        strMoney: String,
        integer: Long,
    ) {
        val result = PgMoney(strMoney)

        assertEquals(integer, result.integer)
    }

    @Test
    fun `plus should add 2 PgMoney instances into a new instance`() {
        val value1 = 526.89
        val value2 = 412.58
        val money1 = PgMoney(value1.toString())
        val money2 = PgMoney(value2.toString())

        val result = money1 + money2

        assertEquals((value1 + value2) * 100, result.integer.toDouble())
    }

    @Test
    fun `minus should subtract 2 PgMoney instances into a new instance`() {
        val value1 = 526.89
        val value2 = 412.58
        val money1 = PgMoney(value1.toString())
        val money2 = PgMoney(value2.toString())

        val result = money1 - money2

        assertEquals((value1 - value2) * 100, result.integer.toDouble())
    }
}
