package com.github.clasicrando.postgresql.column

import com.github.clasicrando.common.connection.use
import com.github.clasicrando.common.result.getAs
import com.github.clasicrando.common.use
import com.github.clasicrando.postgresql.PgConnectionHelper
import com.github.clasicrando.postgresql.type.PgPath
import com.github.clasicrando.postgresql.type.PgPoint
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

        PgConnectionHelper.defaultConnection().use { conn ->
            conn.includePostGisTypes()
            conn.sendPreparedStatement(query, listOf(value)).use { results ->
                assertEquals(1, results.size)
                assertEquals(1, results[0].rowsAffected)
                val rows = results[0].rows.toList()
                assertEquals(1, rows.size)
                assertEquals(value, rows.map { it.getAs<PgPath>("path_col") }.first())
            }
        }
    }

    private suspend fun decodeTest(isClosed: Boolean, isPrepared: Boolean) {
        val value = getPath(isClosed)
        val query = "SELECT '${value.postGisLiteral}'::path;"

        PgConnectionHelper.defaultConnectionWithForcedSimple().use { conn ->
            conn.includePostGisTypes()
            if (isPrepared) {
                conn.sendPreparedStatement(query, emptyList())
            } else {
                conn.sendQuery(query)
            }.use { results ->
                assertEquals(1, results.size)
                assertEquals(1, results[0].rowsAffected)
                val rows = results[0].rows.toList()
                assertEquals(1, rows.size)
                assertEquals(value, rows.map { it.getAs<PgPath>(0)!! }.first())
            }
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
            PgConnectionHelper.defaultConnection().use { conn ->
                conn.sendQuery(POST_GIS_QUERY).use {
                    check(it.first().rows.first().getBoolean(0) == true)
                }
            }
        }
    }
}
