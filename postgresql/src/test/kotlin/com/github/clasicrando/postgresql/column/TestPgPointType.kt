package com.github.clasicrando.postgresql.column

import com.github.clasicrando.common.connection.use
import com.github.clasicrando.common.result.getAs
import com.github.clasicrando.common.use
import com.github.clasicrando.postgresql.PgConnectionHelper
import com.github.clasicrando.postgresql.type.PgPoint
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.BeforeAll
import kotlin.test.Test
import kotlin.test.assertEquals

class TestPgPointType {
    @Test
    fun `encode should accept PgPoint when querying postgresql`() = runBlocking {
        val query = "SELECT $1 point_col;"

        PgConnectionHelper.defaultConnection().use { conn ->
            conn.includePostGisTypes()
            conn.sendPreparedStatement(query, listOf(value)).use { results ->
                assertEquals(1, results.size)
                assertEquals(1, results[0].rowsAffected)
                val rows = results[0].rows.toList()
                assertEquals(1, rows.size)
                assertEquals(value, rows.map { it.getAs<PgPoint>("point_col") }.first())
            }
        }
    }

    private suspend fun decodeTest(isPrepared: Boolean) {
        val query = "SELECT '${value.postGisLiteral}'::point;"

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
                assertEquals(value, rows.map { it.getAs<PgPoint>(0)!! }.first())
            }
        }
    }

    @Test
    fun `decode should return PgPoint when simple querying postgresql point`(): Unit = runBlocking {
        decodeTest(isPrepared = false)
    }

    @Test
    fun `decode should return PgPoint when extended querying postgresql point`(): Unit = runBlocking {
        decodeTest(isPrepared = true)
    }

    companion object {
        private val value = PgPoint(54.89, 84.5)
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