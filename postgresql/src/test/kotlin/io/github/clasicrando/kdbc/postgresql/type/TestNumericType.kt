package io.github.clasicrando.kdbc.postgresql.type

import com.ionspin.kotlin.bignum.decimal.BigDecimal
import io.github.clasicrando.kdbc.core.DEFAULT_KDBC_TEST_TIMEOUT
import io.github.clasicrando.kdbc.core.query.bind
import io.github.clasicrando.kdbc.core.query.fetchScalar
import io.github.clasicrando.kdbc.core.query.query
import io.github.clasicrando.kdbc.core.use
import io.github.clasicrando.kdbc.postgresql.PgConnectionHelper
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Timeout
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import kotlin.test.Test
import kotlin.test.assertEquals

class TestNumericType {
    @ParameterizedTest
    @Timeout(value = DEFAULT_KDBC_TEST_TIMEOUT)
    @ValueSource(strings = ["2548.52489", "85679", "0.534589"])
    fun `encode should accept BiDecimal when querying postgresql`(number: String): Unit =
        runBlocking {
            val value = BigDecimal.parseString(number)
            val query = "SELECT $1 numeric_col;"

            PgConnectionHelper.defaultConnection().use { conn ->
                val bigDecimal =
                    query(query)
                        .bind(value)
                        .fetchScalar<BigDecimal>(conn)
                assertEquals(value, bigDecimal)
            }
        }

    private suspend fun decodeTest(isPrepared: Boolean) {
        val number = "2548.52489"
        val expectedResult = BigDecimal.parseString(number)
        val query = "SELECT $number numeric_col;"

        PgConnectionHelper.defaultConnectionWithForcedSimple().use { conn ->
            val dbQuery =
                if (isPrepared) {
                    query(query)
                } else {
                    query(query)
                }
            val bigDecimal = dbQuery.fetchScalar<BigDecimal>(conn)
            assertEquals(expectedResult, bigDecimal)
        }
    }

    @Test
    @Timeout(value = DEFAULT_KDBC_TEST_TIMEOUT)
    fun `decode should return BigDecimal when simple querying postgresql numeric`(): Unit =
        runBlocking {
            decodeTest(isPrepared = false)
        }

    @Test
    @Timeout(value = DEFAULT_KDBC_TEST_TIMEOUT)
    fun `decode should return BigDecimal when extended querying postgresql numeric`(): Unit =
        runBlocking {
            decodeTest(isPrepared = true)
        }

    @ParameterizedTest
    @Timeout(value = DEFAULT_KDBC_TEST_TIMEOUT)
    @ValueSource(strings = ["2548.52489", "85679", "0.534589"])
    fun `encode should accept Java BiDecimal when querying postgresql`(number: String): Unit =
        runBlocking {
            val value = java.math.BigDecimal(number)
            val query = "SELECT $1 jnumeric_col;"

            PgConnectionHelper.defaultConnection().use { conn ->
                val bigDecimal =
                    query(query)
                        .bind(value)
                        .fetchScalar<java.math.BigDecimal>(conn)
                assertEquals(value, bigDecimal)
            }
        }

    private suspend fun decodeJavaTest(isPrepared: Boolean) {
        val number = "2548.52489"
        val expectedResult = java.math.BigDecimal(number)
        val query = "SELECT $number jnumeric_col;"

        PgConnectionHelper.defaultConnectionWithForcedSimple().use { conn ->
            val dbQuery =
                if (isPrepared) {
                    query(query)
                } else {
                    query(query)
                }
            val bigDecimal = dbQuery.fetchScalar<java.math.BigDecimal>(conn)
            assertEquals(expectedResult, bigDecimal)
        }
    }

    @Test
    @Timeout(value = DEFAULT_KDBC_TEST_TIMEOUT)
    fun `decode should return Java BigDecimal when simple querying postgresql numeric`(): Unit =
        runBlocking {
            decodeJavaTest(isPrepared = false)
        }

    @Test
    @Timeout(value = DEFAULT_KDBC_TEST_TIMEOUT)
    fun `decode should return Java BigDecimal when extended querying postgresql numeric`(): Unit =
        runBlocking {
            decodeJavaTest(isPrepared = true)
        }
}
