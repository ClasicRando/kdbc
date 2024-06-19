package io.github.clasicrando.kdbc.postgresql.column

import io.github.clasicrando.kdbc.core.query.bind
import io.github.clasicrando.kdbc.core.query.fetchScalar
import io.github.clasicrando.kdbc.core.result.getAsNonNull
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

        PgConnectionHelper.defaultAsyncConnection().use { conn ->
            val polygon = conn.createPreparedQuery(query)
                .bind(value)
                .fetchScalar<PgPolygon>()
            assertEquals(value, polygon)
        }
    }

    private suspend fun decodeTest(isPrepared: Boolean) {
        val query = "SELECT '${value.postGisLiteral}'::polygon;"

        PgConnectionHelper.defaultAsyncConnectionWithForcedSimple().use { conn ->
            val polygon = if (isPrepared) {
                conn.createPreparedQuery(query)
            } else {
                conn.createQuery(query)
            }.fetchScalar<PgPolygon>()
            assertEquals(value, polygon)
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
            PgConnectionHelper.defaultAsyncConnection().use { conn ->
                conn.sendSimpleQuery(POST_GIS_QUERY).use {
                    check(it.first().rows.first().getAsNonNull<Boolean>(0))
                }
            }
        }
    }
}
