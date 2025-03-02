package io.github.clasicrando.kdbc.mysql.type

import io.github.clasicrando.kdbc.core.DEFAULT_KDBC_TEST_TIMEOUT
import io.github.clasicrando.kdbc.core.query.bind
import io.github.clasicrando.kdbc.core.query.fetchScalar
import io.github.clasicrando.kdbc.core.query.query
import io.github.clasicrando.kdbc.core.use
import io.github.clasicrando.kdbc.core.useCatching
import io.github.clasicrando.kdbc.mysql.MySqlConnectionHelper
import kotlin.test.assertContains
import kotlin.test.assertNotNull
import kotlin.test.assertTrue
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Timeout
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource

class TestNumbers {
    @ParameterizedTest
    @Timeout(value = DEFAULT_KDBC_TEST_TIMEOUT)
    @ValueSource(booleans = [true, false])
    fun `encode should accept Boolean`(value: Boolean) {
        runBlocking {
            val query = "SELECT ? boolean_column;"

            MySqlConnectionHelper.defaultConnection().use { conn ->
                val actual = query(query).bind(value).fetchScalar<Boolean>(conn)
                Assertions.assertEquals(value, actual)
            }
        }
    }

    @ParameterizedTest
    @Timeout(value = DEFAULT_KDBC_TEST_TIMEOUT)
    @ValueSource(booleans = [true, false])
    fun `decode should return Boolean when text`(value: Boolean) {
        runBlocking {
            val query = "SELECT ${if (value) 1 else 0} boolean_column;"

            MySqlConnectionHelper.defaultConnection().use { conn ->
                val actual = query(query).fetchScalar<Boolean>(conn)
                Assertions.assertEquals(value, actual)
            }
        }
    }

    @ParameterizedTest
    @Timeout(value = DEFAULT_KDBC_TEST_TIMEOUT)
    @ValueSource(bytes = [0, 1, 2])
    fun `decode should return Boolean when binary byte`(value: Byte) {
        runBlocking {
            val query = "SELECT ? byte_column;"

            MySqlConnectionHelper.defaultConnection().use { conn ->
                val actual = query(query).bind(value).fetchScalar<Boolean>(conn)
                Assertions.assertEquals(value != 0.toByte(), actual)
            }
        }
    }

    @ParameterizedTest
    @Timeout(value = DEFAULT_KDBC_TEST_TIMEOUT)
    @ValueSource(shorts = [0, 1, 2])
    fun `decode should return Boolean when binary short`(value: Short) {
        runBlocking {
            val query = "SELECT ? short_column;"

            MySqlConnectionHelper.defaultConnection().use { conn ->
                val actual = query(query).bind(value).fetchScalar<Boolean>(conn)
                Assertions.assertEquals(value != 0.toShort(), actual)
            }
        }
    }

    @ParameterizedTest
    @Timeout(value = DEFAULT_KDBC_TEST_TIMEOUT)
    @ValueSource(ints = [0, 1, 2])
    fun `decode should return Boolean when binary int`(value: Int) {
        runBlocking {
            val query = "SELECT ? int_column;"

            MySqlConnectionHelper.defaultConnection().use { conn ->
                val actual = query(query).bind(value).fetchScalar<Boolean>(conn)
                Assertions.assertEquals(value != 0, actual)
            }
        }
    }

    @ParameterizedTest
    @Timeout(value = DEFAULT_KDBC_TEST_TIMEOUT)
    @ValueSource(longs = [0, 1, 2])
    fun `decode should return Boolean when binary long`(value: Long) {
        runBlocking {
            val query = "SELECT ? int_column;"

            MySqlConnectionHelper.defaultConnection().use { conn ->
                val actual = query(query).bind(value).fetchScalar<Boolean>(conn)
                Assertions.assertEquals(value != 0L, actual)
            }
        }
    }

    @ParameterizedTest
    @Timeout(value = DEFAULT_KDBC_TEST_TIMEOUT)
    @ValueSource(bytes = [-1, 0, 1])
    fun `encode should accept Byte`(value: Byte) {
        runBlocking {
            val query = "SELECT ? byte_column;"

            MySqlConnectionHelper.defaultConnection().use { conn ->
                val actual = query(query).bind(value).fetchScalar<Byte>(conn)
                Assertions.assertEquals(value, actual)
            }
        }
    }

    @ParameterizedTest
    @Timeout(value = DEFAULT_KDBC_TEST_TIMEOUT)
    @ValueSource(bytes = [-1, 0, 1])
    fun `decode should return Byte when text`(value: Byte) {
        runBlocking {
            val query = "SELECT $value byte_column;"

            MySqlConnectionHelper.defaultConnection().use { conn ->
                val actual = query(query).fetchScalar<Byte>(conn)
                Assertions.assertEquals(value, actual)
            }
        }
    }

    @ParameterizedTest
    @Timeout(value = DEFAULT_KDBC_TEST_TIMEOUT)
    @ValueSource(bytes = [-1, 0, 1])
    fun `decode should return Byte when binary byte`(value: Byte) {
        runBlocking {
            val query = "SELECT ? byte_column;"

            MySqlConnectionHelper.defaultConnection().use { conn ->
                val actual = query(query).bind(value).fetchScalar<Byte>(conn)
                Assertions.assertEquals(value, actual)
            }
        }
    }

    @ParameterizedTest
    @Timeout(value = DEFAULT_KDBC_TEST_TIMEOUT)
    @ValueSource(shorts = [-1, 0, 1])
    fun `decode should return Byte when binary short`(value: Short) {
        runBlocking {
            val query = "SELECT ? short_column;"

            MySqlConnectionHelper.defaultConnection().use { conn ->
                val actual = query(query).bind(value).fetchScalar<Byte>(conn)
                Assertions.assertEquals(value.toByte(), actual)
            }
        }
    }

    @ParameterizedTest
    @Timeout(value = DEFAULT_KDBC_TEST_TIMEOUT)
    @ValueSource(shorts = [1205])
    fun `decode should fail when binary short outside of byte range`(value: Short) {
        runBlocking {
            val query = "SELECT ? short_column;"

            val result =
                MySqlConnectionHelper.defaultConnection().useCatching { conn ->
                    query(query).bind(value).fetchScalar<Byte>(conn)
                }
            assertTrue(result.isFailure)
            val ex = result.exceptionOrNull()
            assertNotNull(ex)
            val cause = ex.cause
            assertNotNull(cause)
            assertNotNull(cause.message)
            assertContains(cause.message!!, "Invalid TINYINT value")
        }
    }

    @ParameterizedTest
    @Timeout(value = DEFAULT_KDBC_TEST_TIMEOUT)
    @ValueSource(ints = [-1, 0, 1])
    fun `decode should return Byte when binary int`(value: Int) {
        runBlocking {
            val query = "SELECT ? int_column;"

            MySqlConnectionHelper.defaultConnection().use { conn ->
                val actual = query(query).bind(value).fetchScalar<Byte>(conn)
                Assertions.assertEquals(value.toByte(), actual)
            }
        }
    }

    @ParameterizedTest
    @Timeout(value = DEFAULT_KDBC_TEST_TIMEOUT)
    @ValueSource(ints = [6523569])
    fun `decode should fail when binary int outside of byte range`(value: Int) {
        runBlocking {
            val query = "SELECT ? int_column;"

            val result =
                MySqlConnectionHelper.defaultConnection().useCatching { conn ->
                    query(query).bind(value).fetchScalar<Byte>(conn)
                }
            assertTrue(result.isFailure)
            val ex = result.exceptionOrNull()
            assertNotNull(ex)
            val cause = ex.cause
            assertNotNull(cause)
            assertNotNull(cause.message)
            assertContains(cause.message!!, "Invalid TINYINT value")
        }
    }

    @ParameterizedTest
    @Timeout(value = DEFAULT_KDBC_TEST_TIMEOUT)
    @ValueSource(longs = [-1, 0, 1])
    fun `decode should return Byte when binary long`(value: Long) {
        runBlocking {
            val query = "SELECT ? long_column;"

            MySqlConnectionHelper.defaultConnection().use { conn ->
                val actual = query(query).bind(value).fetchScalar<Byte>(conn)
                Assertions.assertEquals(value.toByte(), actual)
            }
        }
    }

    @ParameterizedTest
    @Timeout(value = DEFAULT_KDBC_TEST_TIMEOUT)
    @ValueSource(longs = [65235695145635L])
    fun `decode should fail when binary long outside of byte range`(value: Long) {
        runBlocking {
            val query = "SELECT ? long_column;"

            val result =
                MySqlConnectionHelper.defaultConnection().useCatching { conn ->
                    query(query).bind(value).fetchScalar<Byte>(conn)
                }
            assertTrue(result.isFailure)
            val ex = result.exceptionOrNull()
            assertNotNull(ex)
            val cause = ex.cause
            assertNotNull(cause)
            assertNotNull(cause.message)
            assertContains(cause.message!!, "Invalid TINYINT value")
        }
    }

    @ParameterizedTest
    @Timeout(value = DEFAULT_KDBC_TEST_TIMEOUT)
    @ValueSource(shorts = [-1, 0, 1])
    fun `encode should accept Short`(value: Short) {
        runBlocking {
            val query = "SELECT ? short_column;"

            MySqlConnectionHelper.defaultConnection().use { conn ->
                val actual = query(query).bind(value).fetchScalar<Short>(conn)
                Assertions.assertEquals(value, actual)
            }
        }
    }

    @ParameterizedTest
    @Timeout(value = DEFAULT_KDBC_TEST_TIMEOUT)
    @ValueSource(bytes = [-1, 0, 1])
    fun `decode should return Byte when text`(value: Short) {
        runBlocking {
            val query = "SELECT $value short_column;"

            MySqlConnectionHelper.defaultConnection().use { conn ->
                val actual = query(query).fetchScalar<Short>(conn)
                Assertions.assertEquals(value, actual)
            }
        }
    }

    @ParameterizedTest
    @Timeout(value = DEFAULT_KDBC_TEST_TIMEOUT)
    @ValueSource(bytes = [-1, 0, 1])
    fun `decode should return Short when binary byte`(value: Byte) {
        runBlocking {
            val query = "SELECT ? byte_column;"

            MySqlConnectionHelper.defaultConnection().use { conn ->
                val actual = query(query).bind(value).fetchScalar<Short>(conn)
                Assertions.assertEquals(value.toShort(), actual)
            }
        }
    }

    @ParameterizedTest
    @Timeout(value = DEFAULT_KDBC_TEST_TIMEOUT)
    @ValueSource(ints = [-1, 0, 1])
    fun `decode should return Short when binary int`(value: Int) {
        runBlocking {
            val query = "SELECT ? int_column;"

            MySqlConnectionHelper.defaultConnection().use { conn ->
                val actual = query(query).bind(value).fetchScalar<Short>(conn)
                Assertions.assertEquals(value.toShort(), actual)
            }
        }
    }

    @ParameterizedTest
    @Timeout(value = DEFAULT_KDBC_TEST_TIMEOUT)
    @ValueSource(ints = [6523569])
    fun `decode should fail when binary int outside of short range`(value: Int) {
        runBlocking {
            val query = "SELECT ? int_column;"

            val result =
                MySqlConnectionHelper.defaultConnection().useCatching { conn ->
                    query(query).bind(value).fetchScalar<Short>(conn)
                }
            assertTrue(result.isFailure)
            val ex = result.exceptionOrNull()
            assertNotNull(ex)
            val cause = ex.cause
            assertNotNull(cause)
            assertNotNull(cause.message)
            assertContains(cause.message!!, "Invalid SMALLINT value")
        }
    }

    @ParameterizedTest
    @Timeout(value = DEFAULT_KDBC_TEST_TIMEOUT)
    @ValueSource(longs = [-1, 0, 1])
    fun `decode should return Short when binary long`(value: Long) {
        runBlocking {
            val query = "SELECT ? long_column;"

            MySqlConnectionHelper.defaultConnection().use { conn ->
                val actual = query(query).bind(value).fetchScalar<Short>(conn)
                Assertions.assertEquals(value.toShort(), actual)
            }
        }
    }

    @ParameterizedTest
    @Timeout(value = DEFAULT_KDBC_TEST_TIMEOUT)
    @ValueSource(longs = [65235695145635L])
    fun `decode should fail when binary long outside of long range`(value: Long) {
        runBlocking {
            val query = "SELECT ? long_column;"

            val result =
                MySqlConnectionHelper.defaultConnection().useCatching { conn ->
                    query(query).bind(value).fetchScalar<Short>(conn)
                }
            assertTrue(result.isFailure)
            val ex = result.exceptionOrNull()
            assertNotNull(ex)
            val cause = ex.cause
            assertNotNull(cause)
            assertNotNull(cause.message)
            assertContains(cause.message!!, "Invalid SMALLINT value")
        }
    }

    @ParameterizedTest
    @Timeout(value = DEFAULT_KDBC_TEST_TIMEOUT)
    @ValueSource(ints = [-1, 0, 1])
    fun `encode should accept Int`(value: Int) {
        runBlocking {
            val query = "SELECT ? int_column;"

            MySqlConnectionHelper.defaultConnection().use { conn ->
                val actual = query(query).bind(value).fetchScalar<Int>(conn)
                Assertions.assertEquals(value, actual)
            }
        }
    }

    @ParameterizedTest
    @Timeout(value = DEFAULT_KDBC_TEST_TIMEOUT)
    @ValueSource(bytes = [-1, 0, 1])
    fun `decode should return Int when text`(value: Int) {
        runBlocking {
            val query = "SELECT $value int_column;"

            MySqlConnectionHelper.defaultConnection().use { conn ->
                val actual = query(query).fetchScalar<Int>(conn)
                Assertions.assertEquals(value, actual)
            }
        }
    }

    @ParameterizedTest
    @Timeout(value = DEFAULT_KDBC_TEST_TIMEOUT)
    @ValueSource(bytes = [-1, 0, 1])
    fun `decode should return Int when binary byte`(value: Byte) {
        runBlocking {
            val query = "SELECT ? byte_column;"

            MySqlConnectionHelper.defaultConnection().use { conn ->
                val actual = query(query).bind(value).fetchScalar<Int>(conn)
                Assertions.assertEquals(value.toInt(), actual)
            }
        }
    }

    @ParameterizedTest
    @Timeout(value = DEFAULT_KDBC_TEST_TIMEOUT)
    @ValueSource(shorts = [-1, 0, 1])
    fun `decode should return Int when binary short`(value: Short) {
        runBlocking {
            val query = "SELECT ? short_column;"

            MySqlConnectionHelper.defaultConnection().use { conn ->
                val actual = query(query).bind(value).fetchScalar<Int>(conn)
                Assertions.assertEquals(value.toInt(), actual)
            }
        }
    }

    @ParameterizedTest
    @Timeout(value = DEFAULT_KDBC_TEST_TIMEOUT)
    @ValueSource(longs = [-1, 0, 1])
    fun `decode should return Int when binary long`(value: Long) {
        runBlocking {
            val query = "SELECT ? long_column;"

            MySqlConnectionHelper.defaultConnection().use { conn ->
                val actual = query(query).bind(value).fetchScalar<Int>(conn)
                Assertions.assertEquals(value.toInt(), actual)
            }
        }
    }

    @ParameterizedTest
    @Timeout(value = DEFAULT_KDBC_TEST_TIMEOUT)
    @ValueSource(longs = [65235695145635L])
    fun `decode should fail when binary long outside of int range`(value: Long) {
        runBlocking {
            val query = "SELECT ? long_column;"

            val result =
                MySqlConnectionHelper.defaultConnection().useCatching { conn ->
                    query(query).bind(value).fetchScalar<Int>(conn)
                }
            assertTrue(result.isFailure)
            val ex = result.exceptionOrNull()
            assertNotNull(ex)
            val cause = ex.cause
            assertNotNull(cause)
            assertNotNull(cause.message)
            assertContains(cause.message!!, "Invalid INT value")
        }
    }

    @ParameterizedTest
    @Timeout(value = DEFAULT_KDBC_TEST_TIMEOUT)
    @ValueSource(longs = [-1, 0, 1])
    fun `encode should accept Long`(value: Long) {
        runBlocking {
            val query = "SELECT ? long_column;"

            MySqlConnectionHelper.defaultConnection().use { conn ->
                val actual = query(query).bind(value).fetchScalar<Long>(conn)
                Assertions.assertEquals(value, actual)
            }
        }
    }

    @ParameterizedTest
    @Timeout(value = DEFAULT_KDBC_TEST_TIMEOUT)
    @ValueSource(bytes = [-1, 0, 1])
    fun `decode should return Int when text`(value: Long) {
        runBlocking {
            val query = "SELECT $value long_column;"

            MySqlConnectionHelper.defaultConnection().use { conn ->
                val actual = query(query).fetchScalar<Long>(conn)
                Assertions.assertEquals(value, actual)
            }
        }
    }

    @ParameterizedTest
    @Timeout(value = DEFAULT_KDBC_TEST_TIMEOUT)
    @ValueSource(bytes = [-1, 0, 1])
    fun `decode should return Long when binary byte`(value: Byte) {
        runBlocking {
            val query = "SELECT ? byte_column;"

            MySqlConnectionHelper.defaultConnection().use { conn ->
                val actual = query(query).bind(value).fetchScalar<Long>(conn)
                Assertions.assertEquals(value.toLong(), actual)
            }
        }
    }

    @ParameterizedTest
    @Timeout(value = DEFAULT_KDBC_TEST_TIMEOUT)
    @ValueSource(shorts = [-1, 0, 1])
    fun `decode should return Long when binary short`(value: Short) {
        runBlocking {
            val query = "SELECT ? short_column;"

            MySqlConnectionHelper.defaultConnection().use { conn ->
                val actual = query(query).bind(value).fetchScalar<Long>(conn)
                Assertions.assertEquals(value.toLong(), actual)
            }
        }
    }

    @ParameterizedTest
    @Timeout(value = DEFAULT_KDBC_TEST_TIMEOUT)
    @ValueSource(ints = [-1, 0, 1])
    fun `decode should return Long when binary int`(value: Int) {
        runBlocking {
            val query = "SELECT ? long_column;"

            MySqlConnectionHelper.defaultConnection().use { conn ->
                val actual = query(query).bind(value).fetchScalar<Long>(conn)
                Assertions.assertEquals(value.toLong(), actual)
            }
        }
    }

    @ParameterizedTest
    @Timeout(value = DEFAULT_KDBC_TEST_TIMEOUT)
    @ValueSource(floats = [-1.0F, 0.0F, 1.0F])
    fun `encode should accept Float`(value: Float) {
        runBlocking {
            val query = "SELECT ? float_column;"

            MySqlConnectionHelper.defaultConnection().use { conn ->
                val actual = query(query).bind(value).fetchScalar<Float>(conn)
                Assertions.assertEquals(value, actual)
            }
        }
    }

    @ParameterizedTest
    @Timeout(value = DEFAULT_KDBC_TEST_TIMEOUT)
    @ValueSource(floats = [-1.0F, 0.0F, 1.0F])
    fun `decode should return Float when text`(value: Float) {
        runBlocking {
            val query = "SELECT CAST($value AS FLOAT) float_column;"

            MySqlConnectionHelper.defaultConnection().use { conn ->
                val actual = query(query).fetchScalar<Float>(conn)
                Assertions.assertEquals(value, actual)
            }
        }
    }

    @ParameterizedTest
    @Timeout(value = DEFAULT_KDBC_TEST_TIMEOUT)
    @ValueSource(doubles = [215214.0215621])
    fun `decode should truncate when binary double outside of float range`(value: Double) {
        val expected = value.toFloat()
        runBlocking {
            val query = "SELECT ? double_column;"

            MySqlConnectionHelper.defaultConnection().use { conn ->
                val actual = query(query).bind(value).fetchScalar<Float>(conn)
                Assertions.assertEquals(expected, actual)
            }
        }
    }

    @ParameterizedTest
    @Timeout(value = DEFAULT_KDBC_TEST_TIMEOUT)
    @ValueSource(doubles = [-1.0, 0.0, 1.0])
    fun `encode should accept Double`(value: Double) {
        runBlocking {
            val query = "SELECT ? double_column;"

            MySqlConnectionHelper.defaultConnection().use { conn ->
                val actual = query(query).bind(value).fetchScalar<Double>(conn)
                Assertions.assertEquals(value, actual)
            }
        }
    }

    @ParameterizedTest
    @Timeout(value = DEFAULT_KDBC_TEST_TIMEOUT)
    @ValueSource(doubles = [-1.0, 0.0, 1.0])
    fun `decode should return Double when text`(value: Double) {
        runBlocking {
            val query = "SELECT CAST($value AS DOUBLE) double_column;"

            MySqlConnectionHelper.defaultConnection().use { conn ->
                val actual = query(query).fetchScalar<Double>(conn)
                Assertions.assertEquals(value, actual)
            }
        }
    }
}
