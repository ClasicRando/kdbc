package io.github.clasicrando.kdbc.postgresql.type

import io.github.clasicrando.kdbc.core.DEFAULT_KDBC_TEST_TIMEOUT
import io.github.clasicrando.kdbc.core.query.bind
import io.github.clasicrando.kdbc.core.query.fetchScalar
import io.github.clasicrando.kdbc.core.query.query
import io.github.clasicrando.kdbc.core.use
import io.github.clasicrando.kdbc.postgresql.PgConnectionHelper
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Timeout
import kotlin.test.Test
import kotlin.test.assertEquals

class TestPgPointType {
    @Test
    @Timeout(value = DEFAULT_KDBC_TEST_TIMEOUT)
    fun `encode should accept PgPoint when querying postgresql`(): Unit =
        runBlocking {
            val query = "SELECT $1 point_col;"

            PgConnectionHelper.defaultConnection().use { conn ->
                val point =
                    query(query)
                        .bind(value)
                        .fetchScalar<PgPoint>(conn)
                assertEquals(value, point)
            }
        }

    private suspend fun decodeTest(isExtended: Boolean) {
        val query = "SELECT '${value.postGisLiteral}'::point;"
        if (isExtended) {
            PgConnectionHelper.defaultConnection()
        } else {
            PgConnectionHelper.defaultConnectionWithForcedSimple()
        }.use { conn ->
            val point = query(query).fetchScalar<PgPoint>(conn)
            assertEquals(value, point)
        }
    }

    @Test
    @Timeout(value = DEFAULT_KDBC_TEST_TIMEOUT)
    fun `decode should return PgPoint when simple querying postgresql point`(): Unit =
        runBlocking {
            decodeTest(isExtended = false)
        }

    @Test
    @Timeout(value = DEFAULT_KDBC_TEST_TIMEOUT)
    fun `decode should return PgPoint when extended querying postgresql point`(): Unit =
        runBlocking {
            decodeTest(isExtended = true)
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
        fun checkPostGis(): Unit =
            runBlocking {
                PgConnectionHelper.defaultConnection().use { conn ->
                    val hasPostGis =
                        query(POST_GIS_QUERY)
                            .fetchScalar<Boolean>(conn)
                    check(hasPostGis ?: false)
                }
            }
    }
}
