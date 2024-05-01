package io.github.clasicrando.kdbc.postgresql.query

import io.github.clasicrando.kdbc.core.connection.use
import io.github.clasicrando.kdbc.core.use
import io.github.clasicrando.kdbc.postgresql.PgConnectionHelper
import kotlinx.coroutines.runBlocking
import kotlin.test.Test
import kotlin.test.assertEquals

class TestPgSuspendingSuspendingPreparedSuspendingQueryBatch {
    @Test
    fun `executeQueries should return StatementResult`(): Unit = runBlocking {
        PgConnectionHelper.defaultSuspendingConnection().use { connection ->
            connection.createPreparedQueryBatch().use { queryBatch ->
                queryBatch.addPreparedQuery("SELECT $ID")
                queryBatch.addPreparedQuery("SELECT $1::text")
                    .bind(TEXT)
                queryBatch.executeQueries().use { statementResult ->
                    assertEquals(2, statementResult.size)

                    val firstResult = statementResult[0]
                    assertEquals(1, firstResult.rowsAffected)
                    assertEquals(ID, firstResult.rows.first().getInt(0))

                    val secondResult = statementResult[1]
                    assertEquals(1, secondResult.rowsAffected)
                    assertEquals(TEXT, secondResult.rows.first().getString(0))
                }
            }
        }
    }

    companion object {
        const val ID = 1
        const val TEXT = "test"
    }
}
