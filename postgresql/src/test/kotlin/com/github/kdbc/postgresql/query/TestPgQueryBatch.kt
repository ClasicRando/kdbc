package com.github.kdbc.postgresql.query

import com.github.kdbc.core.connection.use
import com.github.kdbc.core.use
import com.github.kdbc.postgresql.PgConnectionHelper
import kotlinx.coroutines.runBlocking
import kotlin.test.Test
import kotlin.test.assertEquals

class TestPgQueryBatch {
    @Test
    fun `executeQueries should return StatementResult`(): Unit = runBlocking {
        PgConnectionHelper.defaultConnection().use { connection ->
            connection.createQueryBatch().use { queryBatch ->
                queryBatch.addQuery("SELECT $ID")
                queryBatch.addQuery("SELECT $1::text")
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
