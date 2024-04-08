package io.github.clasicrando.kdbc.postgresql.column

import io.github.clasicrando.kdbc.core.connection.use
import io.github.clasicrando.kdbc.core.result.getAs
import io.github.clasicrando.kdbc.core.use
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

        PgConnectionHelper.defaultConnection().use { conn ->
            conn.sendPreparedStatement(query, listOf(uuid)).use { results ->
                assertEquals(1, results.size)
                assertEquals(1, results[0].rowsAffected)
                val rows = results[0].rows.toList()
                assertEquals(1, rows.size)
                assertEquals(uuid, rows.map { it.getAs<UUID>("uuid_col") }.first())
            }
        }
    }

    private suspend fun decodeTest(isPrepared: Boolean) {
        val uuid = UUID.generateUUID()
        val query = "SELECT '$uuid'::uuid;"

        PgConnectionHelper.defaultConnectionWithForcedSimple().use { conn ->
            if (isPrepared) {
                conn.sendPreparedStatement(query, emptyList())
            } else {
                conn.sendQuery(query)
            }.use { results ->
                assertEquals(1, results.size)
                assertEquals(1, results[0].rowsAffected)
                val rows = results[0].rows.toList()
                assertEquals(1, rows.size)
                assertEquals(uuid, rows.map { it.getAs<UUID>(0)!! }.first())
            }
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
