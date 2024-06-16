package io.github.clasicrando.kdbc.postgresql.column

import io.github.clasicrando.kdbc.core.query.bind
import io.github.clasicrando.kdbc.core.query.fetchScalar
import io.github.clasicrando.kdbc.core.result.getAsNonNull
import io.github.clasicrando.kdbc.core.use
import io.github.clasicrando.kdbc.postgresql.PgConnectionHelper
import io.github.clasicrando.kdbc.postgresql.type.PgPath
import io.github.clasicrando.kdbc.postgresql.type.PgPoint
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import kotlin.test.assertEquals

class TestPgPathType {
    @ParameterizedTest
    @ValueSource(booleans = [true, false])
    fun `encode should accept PgPath when querying postgresql`(isClosed: Boolean) = runBlocking {
        val value = getPath(isClosed)
        val query = "SELECT $1 path_col;"

        PgConnectionHelper.defaultAsyncConnection().use { conn ->
            val path = conn.createPreparedQuery(query)
                .bind(value)
                .fetchScalar<PgPath>()
            assertEquals(value, path)
        }
    }

    private suspend fun decodeTest(isClosed: Boolean, isPrepared: Boolean) {
        val value = getPath(isClosed)
        val query = "SELECT '${value.postGisLiteral}'::path;"

        PgConnectionHelper.defaultAsyncConnectionWithForcedSimple().use { conn ->
            val path = if (isPrepared) {
                conn.createPreparedQuery(query)
            } else {
                conn.createQuery(query)
            }.fetchScalar<PgPath>()
            assertEquals(value, path)
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = [true, false])
    fun `decode should return PgPath when simple querying postgresql path`(isClosed: Boolean): Unit = runBlocking {
        decodeTest(isClosed = isClosed, isPrepared = false)
    }

    @ParameterizedTest
    @ValueSource(booleans = [true, false])
    fun `decode should return PgPath when extended querying postgresql path`(isClosed: Boolean): Unit = runBlocking {
        decodeTest(isClosed = isClosed, isPrepared = true)
    }

    companion object {
        private const val POST_GIS_QUERY = """
            SELECT EXISTS(
                SELECT oid
                FROM pg_extension
                WHERE extname = 'postgis'
            ) post_gis_exists
        """

        private fun getPath(isClosed: Boolean): PgPath {
            return PgPath(
                isClosed = isClosed,
                points = listOf(PgPoint(54.89, 84.5), PgPoint(23.54, 95.24))
            )
        }

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
