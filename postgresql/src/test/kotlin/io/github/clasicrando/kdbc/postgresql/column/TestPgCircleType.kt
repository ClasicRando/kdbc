package io.github.clasicrando.kdbc.postgresql.column

import io.github.clasicrando.kdbc.core.connection.use
import io.github.clasicrando.kdbc.core.query.fetchScalar
import io.github.clasicrando.kdbc.core.use
import io.github.clasicrando.kdbc.postgresql.PgConnectionHelper
import io.github.clasicrando.kdbc.postgresql.type.PgCircle
import io.github.clasicrando.kdbc.postgresql.type.PgPoint
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.BeforeAll
import kotlin.test.Test
import kotlin.test.assertEquals

class TestPgCircleType {
    @Test
    fun `encode should accept PgCircle when querying postgresql`() = runBlocking {
        val query = "SELECT $1 circle_col;"

        PgConnectionHelper.defaultSuspendingConnection().use { conn ->
            conn.includePostGisTypes()
            val circle = conn.createPreparedQuery(query)
                .bind(value)
                .fetchScalar<PgCircle>()
            assertEquals(value, circle)
        }
    }

    private suspend fun decodeTest(isPrepared: Boolean) {
        val query = "SELECT '${value.postGisLiteral}'::circle;"

        PgConnectionHelper.defaultSuspendingConnectionWithForcedSimple().use { conn ->
            conn.includePostGisTypes()
            val circle = if (isPrepared) {
                conn.createPreparedQuery(query)
            } else {
                conn.createQuery(query)
            }.fetchScalar<PgCircle>()
            assertEquals(value, circle)
        }
    }

    @Test
    fun `decode should return PgCircle when simple querying postgresql circle`(): Unit = runBlocking {
        decodeTest(isPrepared = false)
    }

    @Test
    fun `decode should return PgCircle when extended querying postgresql circle`(): Unit = runBlocking {
        decodeTest(isPrepared = true)
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
        fun checkPostGis(): Unit = runBlocking {
            PgConnectionHelper.defaultSuspendingConnection().use { conn ->
                conn.sendSimpleQuery(POST_GIS_QUERY).use {
                    check(it.first().rows.first().getBoolean(0) == true)
                }
            }
        }
    }
}
