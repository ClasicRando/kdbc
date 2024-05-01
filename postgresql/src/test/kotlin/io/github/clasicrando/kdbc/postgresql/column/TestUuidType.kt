package io.github.clasicrando.kdbc.postgresql.column

import io.github.clasicrando.kdbc.core.connection.use
import io.github.clasicrando.kdbc.core.query.fetchScalar
import io.github.clasicrando.kdbc.postgresql.PgConnectionHelper
import kotlinx.coroutines.runBlocking
import kotlinx.uuid.UUID
import kotlinx.uuid.generateUUID
import kotlin.test.Test
import kotlin.test.assertEquals

class TestUuidType {
    @Test
    fun `encode should accept UUID when querying postgresql`() = runBlocking {
        val uuid = UUID.generateUUID()
        val query = "SELECT $1 uuid_col;"

        PgConnectionHelper.defaultSuspendingConnection().use { conn ->
            val value = conn.createPreparedQuery(query)
                .bind(uuid)
                .fetchScalar<UUID>()
            assertEquals(uuid, value)
        }
    }

    private suspend fun decodeTest(isPrepared: Boolean) {
        val uuid = UUID.generateUUID()
        val query = "SELECT '$uuid'::uuid;"

        PgConnectionHelper.defaultSuspendingConnectionWithForcedSimple().use { conn ->
            val value = if (isPrepared) {
                conn.createPreparedQuery(query)
            } else {
                conn.createQuery(query)
            }.fetchScalar<UUID>()
            assertEquals(uuid, value)
        }
    }

    @Test
    fun `decode should return UUID when simple querying postgresql uuid`(): Unit = runBlocking {
        decodeTest(isPrepared = false)
    }

    @Test
    fun `decode should return UUID when extended querying postgresql uuid`(): Unit = runBlocking {
        decodeTest(isPrepared = true)
    }
}
