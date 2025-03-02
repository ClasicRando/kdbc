package io.github.clasicrando.kdbc.postgresql.type

import io.github.clasicrando.kdbc.core.DEFAULT_KDBC_TEST_TIMEOUT
import io.github.clasicrando.kdbc.core.query.bind
import io.github.clasicrando.kdbc.core.query.fetchScalar
import io.github.clasicrando.kdbc.core.query.query
import io.github.clasicrando.kdbc.core.use
import io.github.clasicrando.kdbc.postgresql.PgConnectionHelper
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Timeout
import kotlin.test.Test
import kotlin.test.assertEquals

class TestValueTypeDescription {
    @JvmInline value class Wrapper(val inner: String)

    @Test
    @Timeout(value = DEFAULT_KDBC_TEST_TIMEOUT)
    fun `encode should accept Wrapper when querying postgresql`(): Unit = runBlocking {
        val wrapper = Wrapper("inner")
        val query = "SELECT $1 wrapper_col;"

        PgConnectionHelper.defaultConnection().use { conn ->
            conn.registerValueType<Wrapper>()
            val value = query(query).bind(wrapper).fetchScalar<Wrapper>(conn)
            assertEquals(wrapper, value)
        }
    }

    @Test
    @Timeout(value = DEFAULT_KDBC_TEST_TIMEOUT)
    fun `decode should return Wrapper when querying postgresql text`(): Unit = runBlocking {
        val wrapper = Wrapper("inner")
        val query = "SELECT '${wrapper.inner}' wrapper_col"

        PgConnectionHelper.defaultConnection().use { conn ->
            conn.registerValueType<Wrapper>()
            val value = query(query).fetchScalar<Wrapper>(conn)
            assertEquals(wrapper, value)
        }
    }
}
