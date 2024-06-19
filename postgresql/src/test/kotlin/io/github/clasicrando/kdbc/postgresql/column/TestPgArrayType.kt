package io.github.clasicrando.kdbc.postgresql.column

import io.github.clasicrando.kdbc.core.column.ColumnDecodeError
import io.github.clasicrando.kdbc.core.query.RowParser
import io.github.clasicrando.kdbc.core.query.bind
import io.github.clasicrando.kdbc.core.query.fetchAll
import io.github.clasicrando.kdbc.core.query.fetchScalar
import io.github.clasicrando.kdbc.core.result.DataRow
import io.github.clasicrando.kdbc.core.result.getAsNonNull
import io.github.clasicrando.kdbc.core.use
import io.github.clasicrando.kdbc.postgresql.PgConnectionHelper
import kotlinx.coroutines.runBlocking
import kotlinx.datetime.LocalDateTime
import kotlinx.datetime.TimeZone
import kotlinx.datetime.toInstant
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.assertThrows
import kotlin.test.Test

class TestPgArrayType {
    private fun fieldDescription(pgType: PgType): PgColumnDescription {
        return PgColumnDescription(
            fieldName = "",
            tableOid = 0,
            columnAttribute = 0,
            pgType = pgType,
            dataTypeSize = 0,
            typeModifier = 0,
            formatCode = 0,
        )
    }

    @Test
    fun `decode should return decoded value when valid array literal 1`() {
        val literal = "{1,2,3,4}"
        val pgValue = PgValue.Text(literal, fieldDescription(PgType.Int4Array))

        val result = IntArrayTypeDescription.decode(pgValue)

        Assertions.assertIterableEquals(listOf(1, 2, 3, 4), result)
    }

    @Test
    fun `decode should return decoded value when valid array literal 2`() {
        val literal = "{Test,NULL,Also a test}"
        val pgValue = PgValue.Text(literal, fieldDescription(PgType.TextArray))

        val result = TextArrayTypeDescription.decode(pgValue)

        Assertions.assertIterableEquals(listOf("Test", null, "Also a test"), result)
    }

    @Test
    fun `decode should return decoded value when valid array literal 3`() {
        val literal = "{2023-01-01 22:02:59}"
        val pgValue = PgValue.Text(literal, fieldDescription(PgType.TimestampArray))

        val result = TimestampArrayTypeDescription.decode(pgValue)

        Assertions.assertIterableEquals(
            listOf(LocalDateTime(2023, 1, 1, 22, 2, 59).toInstant(TimeZone.UTC)),
            result,
        )
    }

    @Test
    fun `decode should throw a column decode error when literal is not wrapped by curl braces`() {
        val literal = "1,2,3,4"
        val pgValue = PgValue.Text(literal, fieldDescription(PgType.Int4Array))

        assertThrows<ColumnDecodeError> {
            IntArrayTypeDescription.decode(pgValue)
        }
    }

    @Test
    fun `encode should accept int list when querying postgresql`() = runBlocking {
        val values = listOf(1, 2, 3, 4)
        val query = "SELECT x array_values FROM UNNEST($1) x"

        PgConnectionHelper.defaultAsyncConnection().use { conn ->
            val ints = conn.createPreparedQuery(query)
                .bind(values)
                .fetchAll(object : RowParser<Int> {
                    override fun fromRow(row: DataRow): Int = row.getAsNonNull(0)
                })
            Assertions.assertIterableEquals(values, ints)
        }
    }

    private suspend fun decodeTest(isPrepared: Boolean) {
        val expectedResult = listOf(1, 2, 3, 4)
        val query = "SELECT ARRAY[1,2,3,4]::int[]"

        PgConnectionHelper.defaultAsyncConnectionWithForcedSimple().use { conn ->
            val ints = if (isPrepared) {
                conn.createPreparedQuery(query)
            } else {
                conn.createQuery(query)
            }.fetchScalar<List<Int>>()
            Assertions.assertIterableEquals(expectedResult, ints)
        }
    }

    @Test
    fun `decode should return int list when simple querying postgresql int array`(): Unit = runBlocking {
        decodeTest(isPrepared = false)
    }

    @Test
    fun `decode should return int list when extended querying postgresql int array`(): Unit = runBlocking {
        decodeTest(isPrepared = true)
    }
}
