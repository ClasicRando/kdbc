package io.github.clasicrando.kdbc.postgresql.query

import io.github.clasicrando.kdbc.core.query.bind
import io.github.clasicrando.kdbc.core.query.executeMany
import io.github.clasicrando.kdbc.core.query.fetchMany
import io.github.clasicrando.kdbc.core.query.query
import io.github.clasicrando.kdbc.core.result.getAsNonNull
import io.github.clasicrando.kdbc.core.use
import io.github.clasicrando.kdbc.postgresql.PgConnectionHelper
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking

class TestQueryBatch {
    @Test
    fun `fetchMany should return multiple result sets`(): Unit = runBlocking {
        PgConnectionHelper.defaultConnection().use { connection ->
            val results = queries.fetchMany(connection).toList()

            assertEquals(2, results.size)
            assertEquals(ID, results[0][0].getAsNonNull(0))
            assertEquals(TEXT, results[1][0].getAsNonNull(0))
        }
    }

    @Test
    fun `executeMany should return multiple result sets`(): Unit = runBlocking {
        PgConnectionHelper.defaultConnection().use { connection ->
            val results = queries.executeMany(connection).toList()

            assertEquals(2, results.size)
            assertEquals(1, results[0].rowsAffected)
            assertEquals(1, results[1].rowsAffected)
        }
    }

    companion object {
        const val ID = 1
        const val TEXT = "test"
        private val queries = listOf(query("SELECT $ID"), query("SELECT $1::text").bind(TEXT))
    }
}
