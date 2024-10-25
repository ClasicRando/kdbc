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

class TestPgCircleType {
    @Test
    @Timeout(value = DEFAULT_KDBC_TEST_TIMEOUT)
    fun `encode should accept PgCircle when querying postgresql`(): Unit =
        runBlocking {
            val query = "SELECT $1 circle_col;"

            PgConnectionHelper.defaultConnection().use { conn ->
                val circle =
                    query(query)
                        .bind(value)
                        .fetchScalar<PgCircle>(conn)
                assertEquals(value, circle)
            }
        }

    private suspend fun decodeTest(isExtended: Boolean) {
        val query = "SELECT '${value.postGisLiteral}'::circle;"
        if (isExtended) {
            PgConnectionHelper.defaultConnection()
        } else {
            PgConnectionHelper.defaultConnectionWithForcedSimple()
        }.use { conn ->
            val circle = query(query).fetchScalar<PgCircle>(conn)
            assertEquals(value, circle)
        }
    }

    @Test
    @Timeout(value = DEFAULT_KDBC_TEST_TIMEOUT)
    fun `decode should return PgCircle when simple querying postgresql circle`(): Unit =
        runBlocking {
            decodeTest(isExtended = false)
        }

    @Test
    @Timeout(value = DEFAULT_KDBC_TEST_TIMEOUT)
    fun `decode should return PgCircle when extended querying postgresql circle`(): Unit =
        runBlocking {
            decodeTest(isExtended = true)
        }

    companion object {
        private val value = PgCircle(center = PgPoint(54.89, 84.5), radius = 2.536)
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
