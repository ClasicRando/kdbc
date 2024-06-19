package io.github.clasicrando.kdbc.postgresql.query

import io.github.clasicrando.kdbc.core.query.bind
import io.github.clasicrando.kdbc.core.result.getAsNonNull
import io.github.clasicrando.kdbc.postgresql.PgConnectionHelper
import kotlin.test.Test
import kotlin.test.assertEquals

class TestPgBlockingPreparedQueryBatch {
    @Test
    fun `executeQueries should return StatementResult`() {
        PgConnectionHelper.defaultBlockingConnection().use { connection ->
            connection.createPreparedQueryBatch().use { queryBatch ->
                queryBatch.addPreparedQuery("SELECT $ID")
                queryBatch.addPreparedQuery("SELECT $1::text")
                    .bind(TEXT)
                queryBatch.executeQueries().use { statementResult ->
                    assertEquals(2, statementResult.size)

                    val firstResult = statementResult[0]
                    assertEquals(1, firstResult.rowsAffected)
                    assertEquals(ID, firstResult.rows.first().getAsNonNull(0))

                    val secondResult = statementResult[1]
                    assertEquals(1, secondResult.rowsAffected)
                    assertEquals(TEXT, secondResult.rows.first().getAsNonNull(0))
                }
            }
        }
    }

    companion object {
        const val ID = 1
        const val TEXT = "test"
    }
}
