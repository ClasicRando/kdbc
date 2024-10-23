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
import kotlin.uuid.Uuid

class TestUuidType {
    @Test
    @Timeout(value = DEFAULT_KDBC_TEST_TIMEOUT)
    fun `encode should accept Uuid when querying postgresql`(): Unit =
        runBlocking {
            val uuid = Uuid.random()
            val query = "SELECT $1 uuid_col;"

            PgConnectionHelper.defaultConnection().use { conn ->
                val value =
                    query(query)
                        .bind(uuid)
                        .fetchScalar<Uuid>(conn)
                assertEquals(uuid, value)
            }
        }

    private suspend fun decodeTest(isExtended: Boolean) {
        val uuid = Uuid.random()
        val query = "SELECT '$uuid'::uuid;"
        if (isExtended) {
            PgConnectionHelper.defaultConnection()
        } else {
            PgConnectionHelper.defaultConnectionWithForcedSimple()
        }.use { conn ->
            val value = query(query).fetchScalar<Uuid>(conn)
            assertEquals(uuid, value)
        }
    }

    @Test
    @Timeout(value = DEFAULT_KDBC_TEST_TIMEOUT)
    fun `decode should return Uuid when simple querying postgresql uuid`(): Unit =
        runBlocking {
            decodeTest(isExtended = false)
        }

    @Test
    @Timeout(value = DEFAULT_KDBC_TEST_TIMEOUT)
    fun `decode should return Uuid when extended querying postgresql uuid`(): Unit =
        runBlocking {
            decodeTest(isExtended = true)
        }

    @Test
    @Timeout(value = DEFAULT_KDBC_TEST_TIMEOUT)
    fun `encode should accept Java Uuid when querying postgresql`(): Unit =
        runBlocking {
            val uuid = java.util.UUID.randomUUID()
            val query = "SELECT $1 uuid_col;"

            PgConnectionHelper.defaultConnection().use { conn ->
                val value =
                    query(query)
                        .bind(uuid)
                        .fetchScalar<java.util.UUID>(conn)
                assertEquals(uuid, value)
            }
        }

    private suspend fun decodeJavaTest(isExtended: Boolean) {
        val uuid = java.util.UUID.randomUUID()
        val query = "SELECT '$uuid'::uuid;"
        if (isExtended) {
            PgConnectionHelper.defaultConnection()
        } else {
            PgConnectionHelper.defaultConnectionWithForcedSimple()
        }.use { conn ->
            val value = query(query).fetchScalar<java.util.UUID>(conn)
            assertEquals(uuid, value)
        }
    }

    @Test
    @Timeout(value = DEFAULT_KDBC_TEST_TIMEOUT)
    fun `decode should return Java Uuid when simple querying postgresql uuid`(): Unit =
        runBlocking {
            decodeJavaTest(isExtended = false)
        }

    @Test
    @Timeout(value = DEFAULT_KDBC_TEST_TIMEOUT)
    fun `decode should return Java Uuid when extended querying postgresql uuid`(): Unit =
        runBlocking {
            decodeJavaTest(isExtended = true)
        }
}
