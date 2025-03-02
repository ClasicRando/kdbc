package io.github.clasicrando.kdbc.postgresql.type

import io.github.clasicrando.kdbc.core.DEFAULT_KDBC_TEST_TIMEOUT
import io.github.clasicrando.kdbc.core.query.bind
import io.github.clasicrando.kdbc.core.query.fetchScalar
import io.github.clasicrando.kdbc.core.query.query
import io.github.clasicrando.kdbc.core.use
import io.github.clasicrando.kdbc.postgresql.PgConnectionHelper
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Timeout

class TestPgPolygonType {
    @Test
    @Timeout(value = DEFAULT_KDBC_TEST_TIMEOUT)
    fun `encode should accept PgPolygon when querying postgresql`(): Unit = runBlocking {
        val query = "SELECT $1 polygon_col;"

        PgConnectionHelper.defaultConnection().use { conn ->
            val polygon = query(query).bind(value).fetchScalar<PgPolygon>(conn)
            assertEquals(value, polygon)
        }
    }

    private suspend fun decodeTest(isExtended: Boolean) {
        val query = "SELECT '${value.postGisLiteral}'::polygon;"
        if (isExtended) {
                PgConnectionHelper.defaultConnection()
            } else {
                PgConnectionHelper.defaultConnectionWithForcedSimple()
            }
            .use { conn ->
                val polygon = query(query).fetchScalar<PgPolygon>(conn)
                assertEquals(value, polygon)
            }
    }

    @Test
    @Timeout(value = DEFAULT_KDBC_TEST_TIMEOUT)
    fun `decode should return PgPolygon when simple querying postgresql polygon`(): Unit =
        runBlocking {
            decodeTest(isExtended = false)
        }

    @Test
    @Timeout(value = DEFAULT_KDBC_TEST_TIMEOUT)
    fun `decode should return PgPolygon when extended querying postgresql polygon`(): Unit =
        runBlocking {
            decodeTest(isExtended = true)
        }

    companion object {
        private val value = PgPolygon(points = listOf(PgPoint(54.89, 84.5), PgPoint(23.54, 95.24)))
        private const val POST_GIS_QUERY =
            """
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
                val hasPostGis = query(POST_GIS_QUERY).fetchScalar<Boolean>(conn)
                check(hasPostGis ?: false)
            }
        }
    }
}
