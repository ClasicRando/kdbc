package io.github.clasicrando.kdbc.postgresql.column

import com.ionspin.kotlin.bignum.decimal.BigDecimal
import io.github.clasicrando.kdbc.core.query.bind
import io.github.clasicrando.kdbc.core.query.fetchScalar
import io.github.clasicrando.kdbc.core.use
import io.github.clasicrando.kdbc.postgresql.PgConnectionHelper
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import kotlin.test.Test
import kotlin.test.assertEquals

class TestNumericType {
    @ParameterizedTest
    @ValueSource(strings = ["2548.52489", "85679", "0.534589"])
    fun `encode should accept BiDecimal when querying postgresql`(number: String) = runBlocking {
        val value = BigDecimal.parseString(number)
        val query = "SELECT $1 numeric_col;"

        PgConnectionHelper.defaultConnection().use { conn ->
            val bigDecimal = conn.createPreparedQuery(query)
                .bind(value)
                .fetchScalar<BigDecimal>()
            assertEquals(value, bigDecimal)
        }
    }

    private suspend fun decodeTest(isPrepared: Boolean) {
        val number = "2548.52489"
        val expectedResult = BigDecimal.parseString(number)
        val query = "SELECT $number numeric_col;"

        PgConnectionHelper.defaultConnectionWithForcedSimple().use { conn ->
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

    @ParameterizedTest
    @ValueSource(strings = ["2548.52489", "85679", "0.534589"])
    fun `encode should accept Java BiDecimal when querying postgresql`(number: String) = runBlocking {
        val value = java.math.BigDecimal(number)
        val query = "SELECT $1 jnumeric_col;"

        PgConnectionHelper.defaultConnection().use { conn ->
            val bigDecimal = conn.createPreparedQuery(query)
                .bind(value)
                .fetchScalar<java.math.BigDecimal>()
            assertEquals(value, bigDecimal)
        }
    }

    private suspend fun decodeJavaTest(isPrepared: Boolean) {
        val number = "2548.52489"
        val expectedResult = java.math.BigDecimal(number)
        val query = "SELECT $number jnumeric_col;"

        PgConnectionHelper.defaultConnectionWithForcedSimple().use { conn ->
            val bigDecimal = if (isPrepared) {
                conn.createPreparedQuery(query)
            } else {
                conn.createQuery(query)
            }.fetchScalar<java.math.BigDecimal>()
            assertEquals(expectedResult, bigDecimal)
        }
    }

    @Test
    fun `decode should return Java BigDecimal when simple querying postgresql numeric`(): Unit = runBlocking {
        decodeJavaTest(isPrepared = false)
    }

    @Test
    fun `decode should return Java BigDecimal when extended querying postgresql numeric`(): Unit = runBlocking {
        decodeJavaTest(isPrepared = true)
    }
}
