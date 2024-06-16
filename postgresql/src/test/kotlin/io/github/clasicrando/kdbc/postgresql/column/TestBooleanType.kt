package io.github.clasicrando.kdbc.postgresql.column

import io.github.clasicrando.kdbc.core.query.bind
import io.github.clasicrando.kdbc.core.query.fetchScalar
import io.github.clasicrando.kdbc.core.use
import io.github.clasicrando.kdbc.postgresql.PgConnectionHelper
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import kotlin.test.assertEquals

class TestBooleanType {
    @ParameterizedTest
    @ValueSource(booleans = [true, false])
    fun `encode should accept Boolean when querying postgresql`(value: Boolean) = runBlocking {
        val query = "SELECT $1 bool_col;"

        PgConnectionHelper.defaultSuspendingConnection().use { conn ->
            val boolean = conn.createPreparedQuery(query)
                .bind(value)
                .fetchScalar<Boolean>()
            assertEquals(value, boolean)
        }
    }

    private suspend fun decodeTest(value: Boolean, isPrepared: Boolean) {
        val query = "SELECT $value;"

        PgConnectionHelper.defaultSuspendingConnectionWithForcedSimple().use { conn ->
            val boolean = if (isPrepared) {
                conn.createPreparedQuery(query)
            } else {
                conn.createQuery(query)
            }.fetchScalar<Boolean>()
            assertEquals(value, boolean)
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = [true, false])
    fun `decode should return Boolean when simple querying postgresql bool`(value: Boolean): Unit = runBlocking {
        decodeTest(value = value, isPrepared = false)
    }

    @ParameterizedTest
    @ValueSource(booleans = [true, false])
    fun `decode should return Boolean when extended querying postgresql bool`(value: Boolean): Unit = runBlocking {
        decodeTest(value = value, isPrepared = true)
    }
}