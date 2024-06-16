package io.github.clasicrando.kdbc.postgresql.column

import io.github.clasicrando.kdbc.core.query.bind
import io.github.clasicrando.kdbc.core.query.fetchScalar
import io.github.clasicrando.kdbc.core.result.getAsNonNull
import io.github.clasicrando.kdbc.core.use
import io.github.clasicrando.kdbc.postgresql.PgConnectionHelper
import io.github.clasicrando.kdbc.postgresql.type.PgLineSegment
import io.github.clasicrando.kdbc.postgresql.type.PgPoint
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.BeforeAll
import kotlin.test.Test
import kotlin.test.assertEquals

class TestPgLineSegmentType {
    @Test
    fun `encode should accept PgLineSegment when querying postgresql`() = runBlocking {
        val query = "SELECT $1 lseg_col;"

        PgConnectionHelper.defaultAsyncConnection().use { conn ->
            val lineSegment = conn.createPreparedQuery(query)
                .bind(value)
                .fetchScalar<PgLineSegment>()
            assertEquals(value, lineSegment)
        }
    }

    private suspend fun decodeTest(isPrepared: Boolean) {
        val query = "SELECT '${value.postGisLiteral}'::lseg;"

        PgConnectionHelper.defaultAsyncConnectionWithForcedSimple().use { conn ->
            val lineSegment = if (isPrepared) {
                conn.createPreparedQuery(query)
            } else {
                conn.createQuery(query)
            }.fetchScalar<PgLineSegment>()
            assertEquals(value, lineSegment)
        }
    }

    @Test
    fun `decode should return PgLineSegment when simple querying postgresql lseg`(): Unit = runBlocking {
        decodeTest(isPrepared = false)
    }

    @Test
    fun `decode should return PgLineSegment when extended querying postgresql lseg`(): Unit = runBlocking {
        decodeTest(isPrepared = true)
    }

    companion object {
        private val value = PgLineSegment(
            point1 = PgPoint(54.89, 84.5),
            point2 = PgPoint(23.54, 95.24),
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
