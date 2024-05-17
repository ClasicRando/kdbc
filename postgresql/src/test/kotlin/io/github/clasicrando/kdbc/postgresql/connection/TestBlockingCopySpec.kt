package io.github.clasicrando.kdbc.postgresql.connection

import io.github.clasicrando.kdbc.core.connection.use
import io.github.clasicrando.kdbc.core.connection.useCatching
import io.github.clasicrando.kdbc.core.query.executeClosing
import io.github.clasicrando.kdbc.core.query.fetchScalar
import io.github.clasicrando.kdbc.postgresql.GeneralPostgresError
import io.github.clasicrando.kdbc.postgresql.PgConnectionHelper
import io.github.clasicrando.kdbc.postgresql.copy.CopyFormat
import io.github.clasicrando.kdbc.postgresql.copy.CopyStatement
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

@EnabledIfEnvironmentVariable(named = "PG_COPY_TEST", matches = "true")
class TestBlockingCopySpec {
    @Test
    fun `copyIn should copy all rows`() {
        PgConnectionHelper.defaultBlockingConnection().use {
            it.createQuery("TRUNCATE public.copy_in_test;").executeClosing()
            val copyInStatement = CopyStatement(
                tableName = "copy_in_test",
                schemaName = "public",
                format = CopyFormat.CSV,
            )
            val copyResult = it.copyIn(
                copyInStatement,
                (1..ROW_COUNT).asSequence()
                    .map { i -> "$i,$i Value\n".toByteArray() },
            )
            assertEquals(ROW_COUNT, copyResult.rowsAffected)
            assertEquals("COPY $ROW_COUNT", copyResult.message)
            val count = it.createQuery("SELECT COUNT(*) FROM public.copy_in_test")
                .fetchScalar<Long>()
            assertEquals(ROW_COUNT, count)
        }
    }

    @Test
    fun `copyIn should throw exception when improperly formatted rows`() {
        val result = PgConnectionHelper.defaultBlockingConnection().useCatching {
            it.createQuery("TRUNCATE public.copy_in_test;").executeClosing()
            val copyInStatement = CopyStatement(
                tableName = "copy_in_test",
                schemaName = "public",
                format = CopyFormat.CSV,
            )
            it.copyIn(
                copyInStatement,
                (1..ROW_COUNT).asSequence()
                    .map { i -> "$i,$i Value".toByteArray() },
            )
        }
        assertTrue(result.isFailure)
        assertTrue(result.exceptionOrNull() is GeneralPostgresError)
        PgConnectionHelper.defaultBlockingConnection().use {
            val count = it.createQuery("SELECT COUNT(*) FROM public.copy_in_test;")
                .fetchScalar<Long>()
            assertEquals(0, count)
        }
    }

    @Test
    fun `copyOut should supply all rows from table`() {
        PgConnectionHelper.defaultBlockingConnection().use {
            var rowIndex = 0L
            val copyOutStatement = CopyStatement(
                tableName = "copy_out_test",
                schemaName = "public",
                format = CopyFormat.CSV,
            )
            for (bytes in it.copyOut(copyOutStatement)) {
                rowIndex++
                val str = bytes.toString(Charsets.UTF_8)
                assertEquals("$rowIndex,$rowIndex Value\n", str)
            }
            assertEquals(ROW_COUNT, rowIndex)
        }
    }

    companion object {
        private const val ROW_COUNT = 1_000_000L
        private const val CREATE_COPY_TARGET_TABLE = """
            DROP TABLE IF EXISTS public.copy_in_test;
            CREATE TABLE public.copy_in_test(id int not null, text_field text not null);
        """
        private const val CREATE_COPY_FROM_TABLE = """
            DROP TABLE IF EXISTS public.copy_out_test;
            CREATE TABLE public.copy_out_test(id int not null, text_field text not null);
            INSERT INTO public.copy_out_test(id, text_field)
            SELECT t.t, t.t || ' Value'
            FROM generate_series(1, $ROW_COUNT) t
        """

        @JvmStatic
        @BeforeAll
        fun setup() {
            PgConnectionHelper.defaultBlockingConnection().use {
                it.sendSimpleQuery(CREATE_COPY_TARGET_TABLE)
                it.sendSimpleQuery(CREATE_COPY_FROM_TABLE)
            }
        }
    }
}
