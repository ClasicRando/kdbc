package com.github.kdbc.postgresql.column

import com.github.kdbc.core.connection.use
import com.github.kdbc.core.result.getAs
import com.github.kdbc.core.use
import com.github.kdbc.postgresql.PgConnectionHelper
import com.github.kdbc.postgresql.type.PgBox
import com.github.kdbc.postgresql.type.PgPoint
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.BeforeAll
import kotlin.test.Test
import kotlin.test.assertEquals

class TestPgBoxType {
    @Test
    fun `encode should accept PgBox when querying postgresql`() = runBlocking {
        val query = "SELECT $1 box_col;"

        PgConnectionHelper.defaultConnection().use { conn ->
            conn.includePostGisTypes()
            conn.sendPreparedStatement(query, listOf(value)).use { results ->
                assertEquals(1, results.size)
                assertEquals(1, results[0].rowsAffected)
                val rows = results[0].rows.toList()
                assertEquals(1, rows.size)
                assertEquals(value, rows.map { it.getAs<PgBox>("box_col") }.first())
            }
        }
    }

    private suspend fun decodeTest(isPrepared: Boolean) {
        val query = "SELECT '${value.postGisLiteral}'::box;"

        PgConnectionHelper.defaultConnectionWithForcedSimple().use { conn ->
            conn.includePostGisTypes()
            if (isPrepared) {
                conn.sendPreparedStatement(query, emptyList())
            } else {
                conn.sendQuery(query)
            }.use { results ->
                assertEquals(1, results.size)
                assertEquals(1, results[0].rowsAffected)
                val rows = results[0].rows.toList()
                assertEquals(1, rows.size)
                assertEquals(value, rows.map { it.getAs<PgBox>(0)!! }.first())
            }
        }
    }

    @Test
    fun `decode should return PgBox when simple querying postgresql box`(): Unit = runBlocking {
        decodeTest(isPrepared = false)
    }

    @Test
    fun `decode should return PgBox when extended querying postgresql box`(): Unit = runBlocking {
        decodeTest(isPrepared = true)
    }

    companion object {
        private val value = PgBox(
            high = PgPoint(54.89, 95.24),
            low = PgPoint(23.54, 84.5),
        )
        private const val POST_GIS_QUERY = """
            SELECT EXISTS(
                SELECT oid
                FROM pg_extension
                WHERE extname = 'postgis'
            ) post_gis_exists
        """

        @JvmStatic
        @BeforeAll
        fun checkPostGis(): Unit = runBlocking {
            PgConnectionHelper.defaultConnection().use { conn ->
                conn.sendQuery(POST_GIS_QUERY).use {
                    check(it.first().rows.first().getBoolean(0) == true)
                }
            }
        }
    }
}
