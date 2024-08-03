package io.github.clasicrando.kdbc.postgresql.column

import com.ionspin.kotlin.bignum.decimal.BigDecimal
import io.github.clasicrando.kdbc.core.query.bind
import io.github.clasicrando.kdbc.core.query.fetchScalar
import io.github.clasicrando.kdbc.core.use
import io.github.clasicrando.kdbc.postgresql.PgConnectionHelper
import kotlinx.coroutines.runBlocking
import kotlin.test.Test
import kotlin.test.assertEquals

class TestBigDecimalType {
    @Test
    fun `encode should accept BiDecimal when querying postgresql`() = runBlocking {
        val value = BigDecimal.parseString("2548.52489")
        val query = "SELECT $1 numeric_col;"

        PgConnectionHelper.defaultAsyncConnection().use { conn ->
            val bigDecimal = conn.createPreparedQuery(query)
                .bind(value)
                .fetchScalar<BigDecimal>()
            assertEquals(value, bigDecimal)
        }
    }

    private suspend fun decodeTest(isPrepared: Boolean) {
        val number = "2548.52489"
        val expectedResult = BigDecimal.parseString(number)
        val query = "SELECT $number;"

        PgConnectionHelper.defaultAsyncConnectionWithForcedSimple().use { conn ->
            val bigDecimal = if (isPrepared) {
                conn.createPreparedQuery(query)
            } else {
                conn.createQuery(query)
            }.fetchScalar<BigDecimal>()
            assertEquals(expectedResult, bigDecimal)
        }
    }

    @Test
    fun `decode should return BigDecimal when simple querying postgresql numeric`(): Unit = runBlocking {
        decodeTest(isPrepared = false)
    }

    @Test
    fun `decode should return BigDecimal when extended querying postgresql numeric`(): Unit = runBlocking {
        decodeTest(isPrepared = true)
    }
}
