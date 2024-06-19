package io.github.clasicrando.kdbc.postgresql.column

import io.github.clasicrando.kdbc.core.query.bind
import io.github.clasicrando.kdbc.core.query.fetchScalar
import io.github.clasicrando.kdbc.core.result.getAsNonNull
import io.github.clasicrando.kdbc.core.use
import io.github.clasicrando.kdbc.postgresql.PgConnectionHelper
import io.github.clasicrando.kdbc.postgresql.type.PgBox
import io.github.clasicrando.kdbc.postgresql.type.PgPoint
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.BeforeAll
import kotlin.test.Test
import kotlin.test.assertEquals

class TestPgBoxType {
    @Test
    fun `encode should accept PgBox when querying postgresql`() = runBlocking {
        val query = "SELECT $1 box_col;"

        PgConnectionHelper.defaultAsyncConnection().use { conn ->
            val box = conn.createPreparedQuery(query)
                .bind(value)
                .fetchScalar<PgBox>()
            assertEquals(value, box)
        }
    }

    private suspend fun decodeTest(isPrepared: Boolean) {
        val query = "SELECT '${value.postGisLiteral}'::box;"

        PgConnectionHelper.defaultAsyncConnectionWithForcedSimple().use { conn ->
            val box = if (isPrepared) {
                conn.createPreparedQuery(query)
            } else {
                conn.createQuery(query)
            }.fetchScalar<PgBox>()
            assertEquals(value, box)
        }
    }

    @Test
    fun `decode should return PgBox when simple querying postgresql box`(): Unit = runBlocking {
        decodeTest(isPrepared = false)
    }

    @Test
    fun `decode should return PgBox when extended querying postgresql box`(): Unit = runBlocking {
        decodeTest(isPrepared = true)
    }

    companion object {
        private val value = PgBox(
            high = PgPoint(54.89, 95.24),
            low = PgPoint(23.54, 84.5),
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
