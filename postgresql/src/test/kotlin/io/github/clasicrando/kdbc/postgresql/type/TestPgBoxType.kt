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

class TestPgBoxType {
    @Test
    @Timeout(value = DEFAULT_KDBC_TEST_TIMEOUT)
    fun `encode should accept PgBox when querying postgresql`(): Unit = runBlocking {
        val query = "SELECT $1 box_col;"

        PgConnectionHelper.defaultConnection().use { conn ->
            val box = query(query).bind(value).fetchScalar<PgBox>(conn)
            assertEquals(value, box)
        }
    }

    private suspend fun decodeTest(isExtended: Boolean) {
        val query = "SELECT '${value.postGisLiteral}'::box;"
        if (isExtended) {
                PgConnectionHelper.defaultConnection()
            } else {
                PgConnectionHelper.defaultConnectionWithForcedSimple()
            }
            .use { conn ->
                val box = query(query).fetchScalar<PgBox>(conn)
                assertEquals(value, box)
            }
    }

    @Test
    @Timeout(value = DEFAULT_KDBC_TEST_TIMEOUT)
    fun `decode should return PgBox when simple querying postgresql box`(): Unit = runBlocking {
        decodeTest(isExtended = false)
    }

    @Test
    @Timeout(value = DEFAULT_KDBC_TEST_TIMEOUT)
    fun `decode should return PgBox when extended querying postgresql box`(): Unit = runBlocking {
        decodeTest(isExtended = true)
    }

    companion object {
        private val value = PgBox(high = PgPoint(54.89, 95.24), low = PgPoint(23.54, 84.5))
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
