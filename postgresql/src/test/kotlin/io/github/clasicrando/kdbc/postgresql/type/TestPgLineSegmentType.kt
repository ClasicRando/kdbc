package io.github.clasicrando.kdbc.postgresql.type

import io.github.clasicrando.kdbc.core.DEFAULT_KDBC_TEST_TIMEOUT
import io.github.clasicrando.kdbc.core.query.bind
import io.github.clasicrando.kdbc.core.query.fetchScalar
import io.github.clasicrando.kdbc.core.query.query
import io.github.clasicrando.kdbc.core.result.getAsNonNull
import io.github.clasicrando.kdbc.core.use
import io.github.clasicrando.kdbc.postgresql.PgConnectionHelper
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Timeout
import kotlin.test.Test
import kotlin.test.assertEquals

class TestPgLineSegmentType {
    @Test
    @Timeout(value = DEFAULT_KDBC_TEST_TIMEOUT)
    fun `encode should accept PgLineSegment when querying postgresql`(): Unit =
        runBlocking {
            val query = "SELECT $1 lseg_col;"

            PgConnectionHelper.defaultConnection().use { conn ->
                val lineSegment =
                    query(query)
                        .bind(value)
                        .fetchScalar<PgLineSegment>(conn)
                assertEquals(value, lineSegment)
            }
        }

    private suspend fun decodeTest(isExtended: Boolean) {
        val query = "SELECT '${value.postGisLiteral}'::lseg;"
        if (isExtended) {
            PgConnectionHelper.defaultConnection()
        } else {
            PgConnectionHelper.defaultConnectionWithForcedSimple()
        }.use { conn ->
            val lineSegment = query(query).fetchScalar<PgLineSegment>(conn)
            assertEquals(value, lineSegment)
        }
    }

    @Test
    @Timeout(value = DEFAULT_KDBC_TEST_TIMEOUT)
    fun `decode should return PgLineSegment when simple querying postgresql lseg`(): Unit =
        runBlocking {
            decodeTest(isExtended = false)
        }

    @Test
    @Timeout(value = DEFAULT_KDBC_TEST_TIMEOUT)
    fun `decode should return PgLineSegment when extended querying postgresql lseg`(): Unit =
        runBlocking {
            decodeTest(isExtended = true)
        }

    companion object {
        private val value =
            PgLineSegment(
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
        fun checkPostGis(): Unit =
            runBlocking {
                PgConnectionHelper.defaultConnection().use { conn ->
                    conn.sendSimpleQuery(POST_GIS_QUERY).use {
                        check(
                            it
                                .first()
                                .rows
                                .first()
                                .getAsNonNull<Boolean>(0),
                        )
                    }
                }
            }
    }
}
