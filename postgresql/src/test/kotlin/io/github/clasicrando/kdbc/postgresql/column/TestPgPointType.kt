package io.github.clasicrando.kdbc.postgresql.column

import io.github.clasicrando.kdbc.core.query.bind
import io.github.clasicrando.kdbc.core.query.fetchScalar
import io.github.clasicrando.kdbc.core.result.getAsNonNull
import io.github.clasicrando.kdbc.core.use
import io.github.clasicrando.kdbc.postgresql.PgConnectionHelper
import io.github.clasicrando.kdbc.postgresql.type.PgPoint
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.BeforeAll
import kotlin.test.Test
import kotlin.test.assertEquals

class TestPgPointType {
    @Test
    fun `encode should accept PgPoint when querying postgresql`() = runBlocking {
        val query = "SELECT $1 point_col;"

        PgConnectionHelper.defaultAsyncConnection().use { conn ->
            val point = conn.createPreparedQuery(query)
                .bind(value)
                .fetchScalar<PgPoint>()
            assertEquals(value, point)
        }
    }

    private suspend fun decodeTest(isPrepared: Boolean) {
        val query = "SELECT '${value.postGisLiteral}'::point;"

        PgConnectionHelper.defaultAsyncConnectionWithForcedSimple().use { conn ->
            val point = if (isPrepared) {
                conn.createPreparedQuery(query)
            } else {
                conn.createQuery(query)
            }.fetchScalar<PgPoint>()
            assertEquals(value, point)
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
            PgConnectionHelper.defaultAsyncConnection().use { conn ->
                conn.sendSimpleQuery(POST_GIS_QUERY).use {
                    check(it.first().rows.first().getAsNonNull<Boolean>(0))
                }
            }
        }
    }
}
