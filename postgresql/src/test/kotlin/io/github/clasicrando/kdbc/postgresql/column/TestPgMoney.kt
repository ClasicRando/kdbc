package io.github.clasicrando.kdbc.postgresql.column

import io.github.clasicrando.kdbc.core.query.bind
import io.github.clasicrando.kdbc.core.query.fetchScalar
import io.github.clasicrando.kdbc.core.use
import io.github.clasicrando.kdbc.postgresql.PgConnectionHelper
import io.github.clasicrando.kdbc.postgresql.type.PgMoney
import kotlinx.coroutines.runBlocking
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
            PgMoney.fromString(strMoney)
        }
    }

    @ParameterizedTest
    @ValueSource(doubles = [22.635, 22.63526])
    fun `PgMoney should fail when double has more than 2 values after the decimal place`(
        dblMoney: Double,
    ) {
        assertFailsWith<IllegalStateException> {
            PgMoney(dblMoney)
        }
    }

    @ParameterizedTest
    @CsvSource(
        "-$589.25,-58925,-$589.25",
        "$4152.23,415223,$4152.23",
        "$4152.2,415220,$4152.20",
        "85.96,8596,$85.96",
        "85.9,8590,$85.90",
        "85,8500,$85.00",
    )
    fun `PgMoney should construct new instance when string matches the money regex pattern`(
        strMoney: String,
        integer: Long,
        expectedMoneyStr: String,
    ) {
        val result = PgMoney.fromString(strMoney)

        assertEquals(integer, result.integer)
        assertEquals(expectedMoneyStr, result.toString())
    }

    @ParameterizedTest
    @CsvSource(
        "-589.25,-58925,-$589.25",
        "4152.23,415223,$4152.23",
        "4152.2,415220,$4152.20",
        "85.960,8596,$85.96",
        "85.9,8590,$85.90",
        "85,8500,$85.00",
    )
    fun `PgMoney should construct new instance when double doesn't contain values beyond 2 decimal places`(
        dblMoney: Double,
        integer: Long,
        expectedMoneyStr: String,
    ) {
        val result = PgMoney(dblMoney)

        assertEquals(integer, result.integer)
        assertEquals(expectedMoneyStr, result.toString())
    }

    @Test
    fun `plus should add 2 PgMoney instances into a new instance`() {
        val value1 = 526.89
        val value2 = 412.58
        val money1 = PgMoney(value1)
        val money2 = PgMoney(value2)

        val result = money1 + money2

        assertEquals((value1 + value2) * 100, result.integer.toDouble())
    }

    @Test
    fun `minus should subtract 2 PgMoney instances into a new instance`() {
        val value1 = 526.89
        val value2 = 412.58
        val money1 = PgMoney(value1)
        val money2 = PgMoney(value2)

        val result = money1 - money2

        assertEquals((value1 - value2) * 100, result.integer.toDouble())
    }

    @Test
    fun `encode should accept PgMoney when querying postgresql`() = runBlocking {
        val query = "SELECT $1 money_col;"

        PgConnectionHelper.defaultConnection().use { conn ->
            val money = conn.createPreparedQuery(query)
                .bind(moneyValue)
                .fetchScalar<PgMoney>()
            assertEquals(moneyValue, money)
        }
    }

    private suspend fun decodeTest(isPrepared: Boolean) {
        val query = "SELECT $MONEY_DOUBLE_VALUE::money;"

        PgConnectionHelper.defaultConnectionWithForcedSimple().use { conn ->
            val money = if (isPrepared) {
                conn.createPreparedQuery(query)
            } else {
                conn.createQuery(query)
            }.fetchScalar<PgMoney>()
            assertEquals(moneyValue, money)
        }
    }

    @Test
    fun `decode should return PgMoney when simple querying postgresql money`(): Unit = runBlocking {
        decodeTest(isPrepared = false)
    }

    @Test
    fun `decode should return PgMoney when extended querying postgresql money`(): Unit = runBlocking {
        decodeTest(isPrepared = true)
    }

    companion object {
        private const val MONEY_DOUBLE_VALUE = 71.68
        private val moneyValue = PgMoney(MONEY_DOUBLE_VALUE)
    }
}
