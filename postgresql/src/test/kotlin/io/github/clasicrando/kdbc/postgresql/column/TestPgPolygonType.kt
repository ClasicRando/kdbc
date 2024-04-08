package io.github.clasicrando.kdbc.postgresql.column

import io.github.clasicrando.kdbc.core.connection.use
import io.github.clasicrando.kdbc.core.result.getAs
import io.github.clasicrando.kdbc.core.use
import io.github.clasicrando.kdbc.postgresql.PgConnectionHelper
import io.github.clasicrando.kdbc.postgresql.type.PgPoint
import io.github.clasicrando.kdbc.postgresql.type.PgPolygon
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.BeforeAll
import kotlin.test.Test
import kotlin.test.assertEquals

class TestPgPolygonType {
    @Test
    fun `encode should accept PgPolygon when querying postgresql`() = runBlocking {
        val query = "SELECT $1 polygon_col;"

        PgConnectionHelper.defaultConnection().use { conn ->
            conn.includePostGisTypes()
            conn.sendPreparedStatement(query, listOf(value)).use { results ->
                assertEquals(1, results.size)
                assertEquals(1, results[0].rowsAffected)
                val rows = results[0].rows.toList()
                assertEquals(1, rows.size)
                assertEquals(value, rows.map { it.getAs<PgPolygon>("polygon_col") }.first())
            }
        }
    }

    private suspend fun decodeTest(isPrepared: Boolean) {
        val query = "SELECT '${value.postGisLiteral}'::polygon;"

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
                assertEquals(value, rows.map { it.getAs<PgPolygon>(0)!! }.first())
            }
        }
    }

    @Test
    fun `decode should return PgPolygon when simple querying postgresql polygon`(): Unit = runBlocking {
        decodeTest(isPrepared = false)
    }

    @Test
    fun `decode should return PgPolygon when extended querying postgresql polygon`(): Unit = runBlocking {
        decodeTest(isPrepared = true)
    }

    companion object {
        private val value = PgPolygon(
            points = listOf(PgPoint(54.89, 84.5), PgPoint(23.54, 95.24)),
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
