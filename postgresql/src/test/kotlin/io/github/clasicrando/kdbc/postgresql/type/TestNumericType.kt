package io.github.clasicrando.kdbc.postgresql.type

import io.github.clasicrando.kdbc.core.DEFAULT_KDBC_TEST_TIMEOUT
import io.github.clasicrando.kdbc.core.query.bind
import io.github.clasicrando.kdbc.core.query.fetchScalar
import io.github.clasicrando.kdbc.core.query.query
import io.github.clasicrando.kdbc.core.use
import io.github.clasicrando.kdbc.postgresql.PgConnectionHelper
import java.math.BigDecimal
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Timeout
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource

class TestNumericType {
    @ParameterizedTest
    @Timeout(value = DEFAULT_KDBC_TEST_TIMEOUT)
    @ValueSource(strings = ["2548.52489", "85679", "0.534589"])
    fun `encode should accept BiDecimal when querying postgresql`(number: String): Unit =
        runBlocking {
            val value = BigDecimal(number)
            val query = "SELECT $1 numeric_col;"

            PgConnectionHelper.defaultConnection().use { conn ->
                val bigDecimal = query(query).bind(value).fetchScalar<BigDecimal>(conn)
                assertEquals(value, bigDecimal)
            }
        }

    @Test
    @Timeout(value = DEFAULT_KDBC_TEST_TIMEOUT)
    fun `decode should return BigDecimal when simple querying postgresql numeric`(): Unit =
        runBlocking {
            val number = "2548.52489"
            val expectedResult = BigDecimal(number)
            val query = "SELECT $number numeric_col;"
            PgConnectionHelper.defaultConnectionWithForcedSimple().use { conn ->
                val bigDecimal = query(query).fetchScalar<BigDecimal>(conn)
                assertEquals(expectedResult, bigDecimal)
            }
        }
}
