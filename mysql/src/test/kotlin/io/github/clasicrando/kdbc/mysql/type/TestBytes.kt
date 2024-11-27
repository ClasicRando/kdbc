package io.github.clasicrando.kdbc.mysql.type

import io.github.clasicrando.kdbc.core.DEFAULT_KDBC_TEST_TIMEOUT
import io.github.clasicrando.kdbc.core.query.bind
import io.github.clasicrando.kdbc.core.query.fetchScalar
import io.github.clasicrando.kdbc.core.query.query
import io.github.clasicrando.kdbc.core.use
import io.github.clasicrando.kdbc.mysql.MySqlConnectionHelper
import kotlinx.coroutines.runBlocking
import kotlinx.io.Buffer
import kotlinx.io.readByteArray
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Timeout
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import java.util.UUID
import kotlin.test.assertEquals
import kotlin.uuid.Uuid

class TestBytes {
    @ParameterizedTest
    @Timeout(value = DEFAULT_KDBC_TEST_TIMEOUT)
    @ValueSource(ints = [251256])
    fun `encode should accept ByteArray`(value: Int) {
        val buffer = Buffer()
        buffer.writeInt(value)
        val expected = buffer.readByteArray()
        runBlocking {
            val query = "SELECT ? bytes_column;"

            MySqlConnectionHelper.defaultConnection().use { conn ->
                val actual = query(query).bind(expected).fetchScalar<ByteArray>(conn)
                Assertions.assertArrayEquals(expected, actual)
            }
        }
    }

    @ParameterizedTest
    @Timeout(value = DEFAULT_KDBC_TEST_TIMEOUT)
    @ValueSource(strings = ["3D578"])
    fun `decode should return ByteArray when text`(value: String) {
        val expected = value.toByteArray()
        runBlocking {
            val query = "SELECT CAST('$value' AS BINARY) bytes_column;"

            MySqlConnectionHelper.defaultConnection().use { conn ->
                val actual = query(query).fetchScalar<ByteArray>(conn)
                Assertions.assertArrayEquals(
                    expected,
                    actual,
                    "Expected: ${expected.contentToString()}, Actual: ${actual.contentToString()}",
                )
            }
        }
    }

    @ParameterizedTest
    @Timeout(value = DEFAULT_KDBC_TEST_TIMEOUT)
    @ValueSource(strings = ["08fc4fc7-ac76-11ef-a5db-02449586e07d"])
    fun `encode should accept Uuid`(value: String) {
        val expected = Uuid.parse(value)
        runBlocking {
            val query = "SELECT ? uuid_column;"

            MySqlConnectionHelper.defaultConnection().use { conn ->
                val actual = query(query).bind(expected).fetchScalar<Uuid>(conn)
                assertEquals(expected, actual)
            }
        }
    }

    @ParameterizedTest
    @Timeout(value = DEFAULT_KDBC_TEST_TIMEOUT)
    @ValueSource(strings = ["08fc4fc7-ac76-11ef-a5db-02449586e07d"])
    fun `decode should return Uuid when text`(value: String) {
        val expected = Uuid.parse(value)
        runBlocking {
            val query = "SELECT '$value' uuid_column;"

            MySqlConnectionHelper.defaultConnection().use { conn ->
                val actual = query(query).fetchScalar<Uuid>(conn)
                assertEquals(expected, actual)
            }
        }
    }

    @ParameterizedTest
    @Timeout(value = DEFAULT_KDBC_TEST_TIMEOUT)
    @ValueSource(strings = ["08fc4fc7-ac76-11ef-a5db-02449586e07d"])
    fun `encode should accept UUID`(value: String) {
        val expected = UUID.fromString(value)
        runBlocking {
            val query = "SELECT ? uuid_column;"

            MySqlConnectionHelper.defaultConnection().use { conn ->
                val actual = query(query).bind(expected).fetchScalar<UUID>(conn)
                assertEquals(expected, actual)
            }
        }
    }

    @ParameterizedTest
    @Timeout(value = DEFAULT_KDBC_TEST_TIMEOUT)
    @ValueSource(strings = ["08fc4fc7-ac76-11ef-a5db-02449586e07d"])
    fun `decode should return UUID when text`(value: String) {
        val expected = UUID.fromString(value)
        runBlocking {
            val query = "SELECT '$value' uuid_column;"

            MySqlConnectionHelper.defaultConnection().use { conn ->
                val actual = query(query).fetchScalar<UUID>(conn)
                assertEquals(expected, actual)
            }
        }
    }
}
