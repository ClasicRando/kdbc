package io.github.clasicrando.kdbc.postgresql.connection

import com.github.doyaaaaaken.kotlincsv.dsl.csvReader
import io.github.clasicrando.kdbc.core.connection.useCatching
import io.github.clasicrando.kdbc.core.query.executeClosing
import io.github.clasicrando.kdbc.core.query.fetchScalar
import io.github.clasicrando.kdbc.core.result.getAsNonNull
import io.github.clasicrando.kdbc.postgresql.GeneralPostgresError
import io.github.clasicrando.kdbc.postgresql.IOUtils
import io.github.clasicrando.kdbc.postgresql.PgConnectionHelper
import io.github.clasicrando.kdbc.postgresql.copy.CopyStatement
import kotlinx.io.buffered
import kotlinx.io.files.Path
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
            val copyInStatement = CopyStatement.TableFromCsv(
                schemaName = "public",
                tableName = "copy_in_test",
            )
            val copyResult = it.copyIn(
                copyInStatement,
                (1..ROW_COUNT).asSequence()
                    .map { i -> "$i,$i Value\n".toByteArray() },
            )
            assertEquals(ROW_COUNT_LONG, copyResult.rowsAffected)
            assertEquals("COPY $ROW_COUNT", copyResult.message)
            val count = it.createQuery("SELECT COUNT(*) FROM public.copy_in_test")
                .fetchScalar<Long>()
            assertEquals(ROW_COUNT_LONG, count)
        }
    }

    @Test
    fun `copyIn should copy all rows from file`() {
        val testFilePath = createTempCsvForCopy(rowCount = ROW_COUNT)
        try {
            PgConnectionHelper.defaultBlockingConnection().use {
                it.createQuery("TRUNCATE public.copy_in_test;").executeClosing()
                val copyInStatement = CopyStatement.TableFromCsv(
                    schemaName = "public",
                    tableName = "copy_in_test",
                )
                val copyResult = IOUtils.source(testFilePath).buffered().use { source ->
                    it.copyIn(
                        copyInStatement,
                        source,
                    )
                }
                assertEquals(ROW_COUNT_LONG, copyResult.rowsAffected)
                assertEquals("COPY $ROW_COUNT", copyResult.message)
                val count = it.createQuery("SELECT COUNT(*) FROM public.copy_in_test")
                    .fetchScalar<Long>()
                assertEquals(ROW_COUNT_LONG, count)
            }
        } finally {
            IOUtils.delete(testFilePath, mustExist = false)
        }
    }

    @Test
    fun `copyIn should copy all PgCsvCopyRow values`() {
        PgConnectionHelper.defaultBlockingConnection().use {
            it.createQuery("TRUNCATE public.copy_in_test;").executeClosing()
            val copyInStatement = CopyStatement.TableFromCsv(
                schemaName = "public",
                tableName = "copy_in_test",
            )
            val copyResult = it.copyIn(
                copyInStatement,
                (1..ROW_COUNT).asSequence()
                    .map { i -> CopyInTestRow(id = i, textValue = "$i Value") },
            )
            assertEquals(ROW_COUNT_LONG, copyResult.rowsAffected)
            assertEquals("COPY $ROW_COUNT", copyResult.message)
            val count = it.createQuery("SELECT COUNT(*) FROM public.copy_in_test")
                .fetchScalar<Long>()
            assertEquals(ROW_COUNT_LONG, count)
        }
    }

    @Test
    fun `copyIn should copy all PgBinaryCopyRow values`() {
        PgConnectionHelper.defaultBlockingConnection().use {
            it.createQuery("TRUNCATE public.copy_in_test;").executeClosing()
            val copyInStatement = CopyStatement.TableFromBinary(
                schemaName = "public",
                tableName = "copy_in_test",
            )
            val copyResult = it.copyIn(
                copyInStatement,
                (1..ROW_COUNT).asSequence()
                    .map { i -> CopyInTestRow(id = i, textValue = "$i Value") },
            )
            assertEquals(ROW_COUNT_LONG, copyResult.rowsAffected)
            assertEquals("COPY $ROW_COUNT", copyResult.message)
            val count = it.createQuery("SELECT COUNT(*) FROM public.copy_in_test")
                .fetchScalar<Long>()
            assertEquals(ROW_COUNT_LONG, count)
        }
    }

    @Test
    fun `copyIn should throw exception when improperly formatted rows`() {
        val result = PgConnectionHelper.defaultBlockingConnection().useCatching {
            it.createQuery("TRUNCATE public.copy_in_test;").executeClosing()
            val copyInStatement = CopyStatement.TableFromCsv(
                schemaName = "public",
                tableName = "copy_in_test",
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
    fun `copyOut should supply all rows from table when csv`() {
        PgConnectionHelper.defaultBlockingConnection().use {
            var rowIndex = 0
            val copyOutStatement = CopyStatement.TableToCsv(
                schemaName = "public",
                tableName = "copy_out_test",
            )
            for (row in it.copyOut(copyOutStatement).rows) {
                rowIndex++
                assertEquals(rowIndex, row.getAsNonNull("id"))
                assertEquals("$rowIndex Value", row.getAsNonNull("text_field"))
            }
            assertEquals(ROW_COUNT, rowIndex)
        }
    }

    @Test
    fun `copyOut should supply all rows from query when csv`() {
        PgConnectionHelper.defaultBlockingConnection().use {
            var rowIndex = 0
            val copyOutStatement = CopyStatement.QueryToCsv(
                query = "SELECT * FROM public.copy_out_test",
            )
            for (row in it.copyOut(copyOutStatement).rows) {
                rowIndex++
                assertEquals(rowIndex, row.getAsNonNull("id"))
                assertEquals("$rowIndex Value", row.getAsNonNull("text_field"))
            }
            assertEquals(ROW_COUNT, rowIndex)
        }
    }

    @Test
    fun `copyOut should write all rows from table when csv`() {
        val path = Path(".", "temp", "blocking-copy-out.csv")
        try {
            PgConnectionHelper.defaultBlockingConnection().use {
                var rowIndex = 0
                val copyOutStatement = CopyStatement.TableToCsv(
                    schemaName = "public",
                    tableName = "copy_out_test",
                )
                IOUtils.sink(path).buffered().use { sink ->
                    it.copyOut(copyOutStatement, sink)
                }
                csvReader().open(path.toString()) {
                    for (row in readAllAsSequence()) {
                        rowIndex++
                        assertEquals(rowIndex, row[0].toInt())
                        assertEquals("$rowIndex Value", row[1])
                    }
                }
                assertEquals(ROW_COUNT, rowIndex)
            }
        } finally {
            IOUtils.delete(path = path, mustExist = false)
        }
    }

    @Test
    fun `copyOut should supply all rows from table when binary`() {
        PgConnectionHelper.defaultBlockingConnection().use {
            var rowIndex = 0
            val copyOutStatement = CopyStatement.TableToBinary(
                schemaName = "public",
                tableName = "copy_out_test",
            )
            for (row in it.copyOut(copyOutStatement).rows) {
                rowIndex++
                assertEquals(rowIndex, row.getAsNonNull("id"))
                assertEquals("$rowIndex Value", row.getAsNonNull("text_field"))
            }
            assertEquals(ROW_COUNT, rowIndex)
        }
    }

    companion object {
        private const val ROW_COUNT = 1_000_000
        private const val ROW_COUNT_LONG = ROW_COUNT.toLong()
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
