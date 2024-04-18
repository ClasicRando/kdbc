package io.github.clasicrando.kdbc.postgresql.column

import io.github.clasicrando.kdbc.core.connection.use
import io.github.clasicrando.kdbc.core.result.getList
import io.github.clasicrando.kdbc.core.result.getValueType
import io.github.clasicrando.kdbc.core.use
import io.github.clasicrando.kdbc.postgresql.PgConnectionHelper
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions
import kotlin.test.Test
import kotlin.test.assertEquals

class TestValueType {
    @JvmInline
    value class Id(val value: Long)

    @Test
    fun `encode should accept Id when querying blocking postgresql`() {
        val query = "SELECT $1 value_type;"

        PgConnectionHelper.defaultBlockingConnection().use { conn ->
            conn.registerWrapperValueType<Id>()
            conn.sendPreparedStatement(query, listOf(TEST_ID)).use { results ->
                assertEquals(1, results.size)
                assertEquals(1, results[0].rowsAffected)
                val rows = results[0].rows.toList()
                assertEquals(1, rows.size)
                assertEquals(TEST_ID, rows.map { it.getValueType<Id>("value_type") }.first())
            }
        }
    }

    @Test
    fun `encode should accept Id array when querying blocking postgresql`() {
        val query = "SELECT $1 value_type_array;"

        PgConnectionHelper.defaultBlockingConnection().use { conn ->
            conn.registerWrapperValueType<Id>()
            conn.sendPreparedStatement(query, listOf(TEST_IDS)).use { results ->
                assertEquals(1, results.size)
                assertEquals(1, results[0].rowsAffected)
                val rows = results[0].rows.toList()
                assertEquals(1, rows.size)
                Assertions.assertIterableEquals(
                    TEST_IDS,
                    rows.map { it.getList<Long>("value_type")?.map { id -> Id(id!!) } }
                        .first()
                )
            }
        }
    }

    @Test
    fun `encode should accept Id when querying postgresql`() = runBlocking {
        val query = "SELECT $1 value_type;"

        PgConnectionHelper.defaultConnection().use { conn ->
            conn.registerWrapperValueType<Id>()
            conn.sendPreparedStatement(query, listOf(TEST_ID)).use { results ->
                assertEquals(1, results.size)
                assertEquals(1, results[0].rowsAffected)
                val rows = results[0].rows.toList()
                assertEquals(1, rows.size)
                assertEquals(TEST_ID, rows.map { it.getValueType<Id>("value_type") }.first())
            }
        }
    }

    @Test
    fun `encode should accept Id array when querying postgresql`() = runBlocking {
        val query = "SELECT $1 value_type_array;"

        PgConnectionHelper.defaultConnection().use { conn ->
            conn.registerWrapperValueType<Id>()
            conn.sendPreparedStatement(query, listOf(TEST_IDS)).use { results ->
                assertEquals(1, results.size)
                assertEquals(1, results[0].rowsAffected)
                val rows = results[0].rows.toList()
                assertEquals(1, rows.size)
                Assertions.assertIterableEquals(
                    TEST_IDS,
                    rows.map { it.getList<Long>("value_type")?.map { id -> Id(id!!) } }
                        .first()
                )
            }
        }
    }

    companion object {
        val TEST_ID = Id(19203)
        val TEST_IDS = listOf(Id(528), Id(25489))
    }
}
