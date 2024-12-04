package io.github.clasicrando.kdbc.mysql.connection

import io.github.clasicrando.kdbc.core.query.execute
import io.github.clasicrando.kdbc.core.query.fetchScalar
import io.github.clasicrando.kdbc.core.query.query
import io.github.clasicrando.kdbc.core.statement.CsvDataRow
import io.github.clasicrando.kdbc.core.use
import io.github.clasicrando.kdbc.core.useCatching
import io.github.clasicrando.kdbc.mysql.MySqlConnectionHelper
import io.github.clasicrando.kdbc.mysql.load.LoadLocalFileStatement
import java.nio.file.Files
import java.nio.file.Path
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll

class TestLoadLocalSpec {
    @BeforeTest
    fun init() {
        runBlocking {
            MySqlConnectionHelper.defaultConnection().use { query(CLEAN_SCRIPT).execute(it) }
        }
    }

    @Test
    fun `loadLocalFile should accept valid csv file`(): Unit = runBlocking {
        val statement = LoadLocalFileStatement(TEST_TABLE, skipLines = 0)
        val result =
            MySqlConnectionHelper.defaultConnection().use { it.loadLocalFile(statement, filePath) }
        assertEquals(result.rowsAffected, ROW_COUNT)

        val count =
            MySqlConnectionHelper.defaultConnection().use {
                query("SELECT COUNT(*) FROM $TEST_TABLE").fetchScalar<Long>(it)
            }

        assertNotNull(count)
        assertEquals(ROW_COUNT, count)
    }

    data class LocalLoadRow(val id: Int, val textCol: String) : CsvDataRow {
        override val values: List<Any?> = listOf(id, textCol)
    }

    @Test
    fun `loadLocalFile should accept valid csv rows`(): Unit = runBlocking {
        val statement = LoadLocalFileStatement(TEST_TABLE)
        val records = (1..ROW_COUNT).asFlow().map { i -> LocalLoadRow(i.toInt(), "$i Value") }
        val result =
            MySqlConnectionHelper.defaultConnection().use { it.loadLocalData(statement, records) }
        assertEquals(result.rowsAffected, ROW_COUNT)

        val count =
            MySqlConnectionHelper.defaultConnection().use {
                query("SELECT COUNT(*) FROM $TEST_TABLE").fetchScalar<Long>(it)
            }

        assertNotNull(count)
        assertEquals(ROW_COUNT, count)
    }

    @Test
    fun `loadLocalFile should rollback when inTransaction is false and bad row`(): Unit =
        runBlocking {
            val statement = LoadLocalFileStatement(TEST_TABLE)
            val records =
                (1..ROW_COUNT).asFlow().map { i ->
                    if (i == ROW_COUNT) {
                        error("Bad Row")
                    }
                    LocalLoadRow(i.toInt(), "$i Value")
                }
            val result =
                MySqlConnectionHelper.defaultConnection().useCatching {
                    it.loadLocalData(statement, records, withTransaction = true)
                }

            assertTrue(result.isFailure)
            val ex = result.exceptionOrNull()
            assertNotNull(ex)
            assertNotNull(ex.message)
            assertEquals("Bad Row", ex.message)

            val count =
                MySqlConnectionHelper.defaultConnection().use {
                    query("SELECT COUNT(*) FROM $TEST_TABLE").fetchScalar<Long>(it)
                }

            assertNotNull(count)
            assertEquals(0, count)
        }

    companion object {
        private val filePath = Path.of(".", "temp", "local_load.csv")
        private const val ROW_COUNT = 1_000_000L
        private const val TEST_TABLE = "load_local_test"
        private const val SETUP_SCRIPT =
            """
            DROP TABLE IF EXISTS $TEST_TABLE;
            CREATE TABLE $TEST_TABLE(id int, text_col text);
            """
        private const val CLEAN_SCRIPT = "TRUNCATE TABLE $TEST_TABLE;"

        @JvmStatic
        @BeforeAll
        fun setup() {
            runBlocking {
                MySqlConnectionHelper.defaultConnection().use { query(SETUP_SCRIPT).execute(it) }
            }
            createTempCsvForLoad(filePath, ROW_COUNT)
        }

        @JvmStatic
        @AfterAll
        fun tearDown() {
            Files.deleteIfExists(filePath)
        }
    }
}
