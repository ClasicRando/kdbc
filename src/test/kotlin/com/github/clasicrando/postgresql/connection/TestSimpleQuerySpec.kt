package com.github.clasicrando.postgresql.connection

import com.github.clasicrando.common.connection.use
import com.github.clasicrando.common.result.getInt
import com.github.clasicrando.postgresql.PgConnectionHelper
import kotlinx.coroutines.runBlocking
import kotlin.test.Test
import kotlin.test.assertEquals

class TestSimpleQuerySpec {
    @Test
    fun test(): Unit = runBlocking {
        PgConnectionHelper.defaultConnection().use {
            val queries = """
                CALL public.test_proc(null);
                SELECT 1 test_i;
            """.trimIndent()
            val result = it.sendQuery(queries).toList()
            assertEquals(2, result.size)
            assertEquals(0, result[0].rowsAffected)
            assertEquals(4, result[0].rows.firstOrNull()?.getInt(0))
            assertEquals(1, result[1].rowsAffected)
            assertEquals(1, result[1].rows.firstOrNull()?.getInt("test_i"))
        }
    }
}
