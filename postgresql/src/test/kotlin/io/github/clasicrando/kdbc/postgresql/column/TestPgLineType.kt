package io.github.clasicrando.kdbc.postgresql.column

import io.github.clasicrando.kdbc.core.connection.use
import io.github.clasicrando.kdbc.core.query.fetchScalar
import io.github.clasicrando.kdbc.core.use
import io.github.clasicrando.kdbc.postgresql.PgConnectionHelper
import io.github.clasicrando.kdbc.postgresql.type.PgLine
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.BeforeAll
import kotlin.test.Test
import kotlin.test.assertEquals

class TestPgLineType {
    @Test
    fun `encode should accept PgLine when querying postgresql`() = runBlocking {
        val query = "SELECT $1 line_col;"

        PgConnectionHelper.defaultSuspendingConnection().use { conn ->
            conn.includePostGisTypes()
            val line = conn.createPreparedQuery(query)
                .bind(value)
                .fetchScalar<PgLine>()
            assertEquals(value, line)
        }
    }

    private suspend fun decodeTest(isPrepared: Boolean) {
        val query = "SELECT '${value.postGisLiteral}'::line;"

        PgConnectionHelper.defaultSuspendingConnectionWithForcedSimple().use { conn ->
            conn.includePostGisTypes()
            val line = if (isPrepared) {
                conn.createPreparedQuery(query)
            } else {
                conn.createQuery(query)
            }.fetchScalar<PgLine>()
            assertEquals(value, line)
        }
    }

    @Test
    fun `decode should return PgLine when simple querying postgresql line`(): Unit = runBlocking {
        decodeTest(isPrepared = false)
    }

    @Test
    fun `decode should return PgLine when extended querying postgresql line`(): Unit = runBlocking {
        decodeTest(isPrepared = true)
    }

    companion object {
        private val value = PgLine(54.89, 84.5, 74.526)
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
