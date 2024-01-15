package com.github.clasicrando.postgresql.connection

import com.github.clasicrando.common.connection.use
import com.github.clasicrando.common.result.getInt
import com.github.clasicrando.common.result.getString
import com.github.clasicrando.postgresql.PgConnectionHelper
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import kotlin.test.Test
import kotlin.test.assertEquals

class TestPipelineQuerySpec {
    @Test
    fun `pipelineQueries should return multiple results with auto commit`(): Unit = runBlocking {
        PgConnectionHelper.defaultConnection().use {  connection ->
            val results = connection.pipelineQueries(
                "SELECT $1 i" to listOf(1),
                "SELECT $1 t" to listOf("Pipeline Query"),
            ).toList()
            assertEquals(2, results.size)
            assertEquals(1, results[0].rowsAffected)
            assertEquals(1, results[0].rows.first().getInt(0))
            assertEquals(1, results[1].rowsAffected)
            assertEquals("Pipeline Query", results[1].rows.first().getString(0))
        }
    }
}
