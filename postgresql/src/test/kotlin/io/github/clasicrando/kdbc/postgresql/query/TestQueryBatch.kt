package io.github.clasicrando.kdbc.postgresql.query

import io.github.clasicrando.kdbc.core.query.bind
import io.github.clasicrando.kdbc.core.query.query
import io.github.clasicrando.kdbc.core.result.Either
import io.github.clasicrando.kdbc.core.result.getAsNonNull
import io.github.clasicrando.kdbc.core.use
import io.github.clasicrando.kdbc.postgresql.PgConnectionHelper
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlinx.coroutines.runBlocking

class TestQueryBatch {
    @Test
    fun `executeQueries should return StatementResult`(): Unit = runBlocking {
        PgConnectionHelper.defaultConnection().use { connection ->
            val statementResult =
                connection.executeQueryBatch(
                    batch = listOf(
                        query("SELECT $ID"),
                        query("SELECT $1::text").bind(TEXT),
                    )
                )
            var results = 0
            statementResult.collect {
                when (it) {
                    is Either.Left -> {
                        results += 1
                        assertEquals(1, it.inner.rowsAffected)
                    }
                    is Either.Right -> {
                        if (results == 0) {
                            assertEquals(ID, it.inner.getAsNonNull(0))
                        } else if (results == 1) {
                            assertEquals(TEXT, it.inner.getAsNonNull(0))
                        }
                    }
                }
            }
            assertEquals(2, results)
        }
    }

    companion object {
        const val ID = 1
        const val TEXT = "test"
    }
}
