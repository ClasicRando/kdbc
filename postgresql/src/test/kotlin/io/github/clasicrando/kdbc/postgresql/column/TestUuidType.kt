package io.github.clasicrando.kdbc.postgresql.column

import io.github.clasicrando.kdbc.core.query.bind
import io.github.clasicrando.kdbc.core.query.fetchScalar
import io.github.clasicrando.kdbc.core.use
import io.github.clasicrando.kdbc.postgresql.PgConnectionHelper
import kotlinx.coroutines.runBlocking
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.uuid.Uuid

class TestUuidType {
    @Test
    fun `encode should accept Uuid when querying postgresql`() = runBlocking {
        val uuid = Uuid.random()
        val query = "SELECT $1 uuid_col;"

        PgConnectionHelper.defaultConnection().use { conn ->
            val value = conn.createPreparedQuery(query)
                .bind(uuid)
                .fetchScalar<Uuid>()
            assertEquals(uuid, value)
        }
    }

    private suspend fun decodeTest(isPrepared: Boolean) {
        val uuid = Uuid.random()
        val query = "SELECT '$uuid'::uuid;"

        PgConnectionHelper.defaultConnectionWithForcedSimple().use { conn ->
            val value = if (isPrepared) {
                conn.createPreparedQuery(query)
            } else {
                conn.createQuery(query)
            }.fetchScalar<Uuid>()
            assertEquals(uuid, value)
        }
    }

    @Test
    fun `decode should return Uuid when simple querying postgresql uuid`(): Unit = runBlocking {
        decodeTest(isPrepared = false)
    }

    @Test
    fun `decode should return Uuid when extended querying postgresql uuid`(): Unit = runBlocking {
        decodeTest(isPrepared = true)
    }
}
