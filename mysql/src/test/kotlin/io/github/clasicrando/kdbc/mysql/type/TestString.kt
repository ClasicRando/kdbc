package io.github.clasicrando.kdbc.mysql.type

import io.github.clasicrando.kdbc.core.DEFAULT_KDBC_TEST_TIMEOUT
import io.github.clasicrando.kdbc.core.query.bind
import io.github.clasicrando.kdbc.core.query.execute
import io.github.clasicrando.kdbc.core.query.fetchScalar
import io.github.clasicrando.kdbc.core.query.query
import io.github.clasicrando.kdbc.core.type.Json
import io.github.clasicrando.kdbc.core.use
import io.github.clasicrando.kdbc.mysql.MySqlConnectionHelper
import java.math.BigDecimal
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Timeout
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.EnumSource
import org.junit.jupiter.params.provider.ValueSource

class TestString {
    @ParameterizedTest
    @Timeout(value = DEFAULT_KDBC_TEST_TIMEOUT)
    @ValueSource(strings = ["This is a test"])
    fun `encode should accept String`(value: String) {
        runBlocking {
            val query = "SELECT ? string_column;"

            MySqlConnectionHelper.defaultConnection().use { conn ->
                val actual = query(query).bind(value).fetchScalar<String>(conn)
                Assertions.assertEquals(value, actual)
            }
        }
    }

    @ParameterizedTest
    @Timeout(value = DEFAULT_KDBC_TEST_TIMEOUT)
    @ValueSource(strings = ["This is a test"])
    fun `decode should return String when text`(value: String) {
        runBlocking {
            val query = "SELECT '$value' string_column;"

            MySqlConnectionHelper.defaultConnection().use { conn ->
                val actual = query(query).fetchScalar<String>(conn)
                Assertions.assertEquals(value, actual)
            }
        }
    }

    @ParameterizedTest
    @Timeout(value = DEFAULT_KDBC_TEST_TIMEOUT)
    @ValueSource(strings = ["235266.253"])
    fun `encode should accept BigDecimal`(value: String) {
        val expected = BigDecimal(value)
        runBlocking {
            val query = "SELECT ? decimal_column;"

            MySqlConnectionHelper.defaultConnection().use { conn ->
                val actual = query(query).bind(expected).fetchScalar<BigDecimal>(conn)
                Assertions.assertEquals(expected, actual)
            }
        }
    }

    @ParameterizedTest
    @Timeout(value = DEFAULT_KDBC_TEST_TIMEOUT)
    @ValueSource(strings = ["235266.253"])
    fun `decode should return BigDecimal when text`(value: String) {
        val expected = BigDecimal(value)
        runBlocking {
            val query = "SELECT CAST('$value' AS DECIMAL(9,3)) decimal_column;"

            MySqlConnectionHelper.defaultConnection().use { conn ->
                val actual = query(query).fetchScalar<BigDecimal>(conn)
                Assertions.assertEquals(expected, actual)
            }
        }
    }

    @Suppress("unused")
    enum class TestEnum {
        Test,
        Field,
    }

    @ParameterizedTest
    @Timeout(value = DEFAULT_KDBC_TEST_TIMEOUT)
    @EnumSource(value = TestEnum::class)
    fun `encode should accept custom enum`(value: TestEnum) {
        runBlocking {
            MySqlConnectionHelper.defaultConnection().use { conn ->
                query(CREATE_ENUM_TABLE).execute(conn)
                conn.registerEnumTypeDescription<TestEnum>()
                val actual =
                    query("SELECT enum_field FROM $TABLE_NAME WHERE enum_field = ?;")
                        .bind(value)
                        .fetchScalar<TestEnum>(conn)
                Assertions.assertEquals(value, actual)
            }
        }
    }

    @ParameterizedTest
    @Timeout(value = DEFAULT_KDBC_TEST_TIMEOUT)
    @EnumSource(value = TestEnum::class)
    fun `decode should return custom enum when text`(value: TestEnum) {
        runBlocking {
            val query = "SELECT enum_field FROM $TABLE_NAME WHERE enum_field = '${value.name}';"

            MySqlConnectionHelper.defaultConnection().use { conn ->
                query(CREATE_ENUM_TABLE).execute(conn)
                conn.registerEnumTypeDescription<TestEnum>()
                val actual = query(query).fetchScalar<TestEnum>(conn)
                Assertions.assertEquals(value, actual)
            }
        }
    }

    @ParameterizedTest
    @Timeout(value = DEFAULT_KDBC_TEST_TIMEOUT)
    @ValueSource(strings = ["{\"test\": true}"])
    fun `encode should accept Json`(value: String) {
        val json: Json = Json.Text(value)
        runBlocking {
            val query = "SELECT ? json_column;"

            MySqlConnectionHelper.defaultConnection().use { conn ->
                val actual = query(query).bind(json).fetchScalar<Json>(conn)
                Assertions.assertEquals(json, actual)
            }
        }
    }

    @ParameterizedTest
    @Timeout(value = DEFAULT_KDBC_TEST_TIMEOUT)
    @ValueSource(strings = ["{\"test\": true}"])
    fun `decode should return Json when text`(value: String) {
        val json: Json = Json.Text(value)
        runBlocking {
            val query = "SELECT CAST('$value' AS JSON) json_column;"

            MySqlConnectionHelper.defaultConnection().use { conn ->
                val actual = query(query).fetchScalar<Json>(conn)
                Assertions.assertEquals(json, actual)
            }
        }
    }

    @ParameterizedTest
    @Timeout(value = DEFAULT_KDBC_TEST_TIMEOUT)
    @ValueSource(strings = ["{\"test\": true}"])
    fun `encode should accept JsonText`(value: String) {
        val json = Json.Text(value)
        runBlocking {
            val query = "SELECT ? json_text_column;"

            MySqlConnectionHelper.defaultConnection().use { conn ->
                val actual = query(query).bind(json).fetchScalar<Json.Text>(conn)
                Assertions.assertEquals(json, actual)
            }
        }
    }

    @ParameterizedTest
    @Timeout(value = DEFAULT_KDBC_TEST_TIMEOUT)
    @ValueSource(strings = ["{\"test\": true}"])
    fun `decode should return JsonText when text`(value: String) {
        val json = Json.Text(value)
        runBlocking {
            val query = "SELECT CAST('$value' AS JSON) json_text_column;"

            MySqlConnectionHelper.defaultConnection().use { conn ->
                val actual = query(query).fetchScalar<Json.Text>(conn)
                Assertions.assertEquals(json, actual)
            }
        }
    }

    @ParameterizedTest
    @Timeout(value = DEFAULT_KDBC_TEST_TIMEOUT)
    @ValueSource(strings = ["{\"test\": true}"])
    fun `encode should accept JsonBytes`(value: String) {
        val json = Json.Bytes(value.toByteArray())
        runBlocking {
            val query = "SELECT ? json_bytes_column;"

            MySqlConnectionHelper.defaultConnection().use { conn ->
                val actual = query(query).bind(json).fetchScalar<Json.Bytes>(conn)
                Assertions.assertEquals(json, actual)
            }
        }
    }

    @ParameterizedTest
    @Timeout(value = DEFAULT_KDBC_TEST_TIMEOUT)
    @ValueSource(strings = ["{\"test\": true}"])
    fun `decode should return JsonBytes when text`(value: String) {
        val json = Json.Bytes(value.toByteArray())
        runBlocking {
            val query = "SELECT CAST('$value' AS JSON) json_bytes_column;"

            MySqlConnectionHelper.defaultConnection().use { conn ->
                val actual = query(query).fetchScalar<Json.Bytes>(conn)
                Assertions.assertEquals(json, actual)
            }
        }
    }

    companion object {
        private const val TABLE_NAME = "enum_test"
        private const val CREATE_ENUM_TABLE =
            """
                DROP TABLE IF EXISTS $TABLE_NAME;
                CREATE TABLE $TABLE_NAME(enum_field ENUM('Test','Field'));
                INSERT INTO $TABLE_NAME VALUES ('Test'),('Field');
            """
    }
}
