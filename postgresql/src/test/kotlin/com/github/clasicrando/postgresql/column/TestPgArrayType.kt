package com.github.clasicrando.postgresql.column

import com.github.clasicrando.common.column.ColumnDecodeError
import com.github.clasicrando.common.connection.use
import com.github.clasicrando.common.result.getAs
import com.github.clasicrando.postgresql.PgConnectionHelper
import com.github.clasicrando.postgresql.row.PgColumnDescription
import kotlinx.coroutines.runBlocking
import kotlinx.datetime.LocalDateTime
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.assertThrows
import kotlin.test.Test
import kotlin.test.assertEquals

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
        val decoder = arrayTypeDecoder(intTypeDecoder)
        val pgValue = PgValue.Text(literal, fieldDescription(PgType.Int4Array))

        val result = decoder.decode(pgValue)

        Assertions.assertIterableEquals(listOf(1, 2, 3, 4), result)
    }

    @Test
    fun `decode should return decoded value when valid array literal 2`() {
        val literal = "{Test,NULL,Also a test}"
        val decoder = arrayTypeDecoder(stringTypeDecoder)
        val pgValue = PgValue.Text(literal, fieldDescription(PgType.TextArray))

        val result = decoder.decode(pgValue)

        Assertions.assertIterableEquals(listOf("Test", null, "Also a test"), result)
    }

    @Test
    fun `decode should return decoded value when valid array literal 3`() {
        val literal = "{2023-01-01 22:02:59}"
        val decoder = arrayTypeDecoder(localDateTimeTypeDecoder)
        val pgValue = PgValue.Text(literal, fieldDescription(PgType.TimestampArray))

        val result = decoder.decode(pgValue)

        Assertions.assertIterableEquals(
            listOf(LocalDateTime(2023, 1, 1, 22, 2, 59)),
            result,
        )
    }

    @Test
    fun `decode should throw a column decode error when literal is not wrapped by curl braces`() {
        val literal = "1,2,3,4"
        val decoder = arrayTypeDecoder(intTypeDecoder)
        val pgValue = PgValue.Text(literal, fieldDescription(PgType.Int4Array))

        assertThrows<ColumnDecodeError> {
            decoder.decode(pgValue)
        }
    }

    @Test
    fun `encode should accept int list when querying postgresql`() = runBlocking {
        val values = listOf(1, 2, 3, 4)
        val query = "SELECT x FROM UNNEST($1) x"

        val result = PgConnectionHelper.defaultConnection().use {
            it.sendPreparedStatement(query, listOf(values))
        }.toList()

        assertEquals(1, result.size)
        assertEquals(4, result[0].rowsAffected)
        val rows = result[0].rows.toList()
        assertEquals(4, rows.size)
        Assertions.assertIterableEquals(values, rows.map { it.getAs<Int>(0) })
    }

    private suspend inline fun `decode should return int list when querying postgresql int array`(
        isPrepared: Boolean,
    ) {
        val expectedResult = listOf(1, 2, 3, 4)
        val query = "SELECT ARRAY[1,2,3,4]::int[]"

        val result = PgConnectionHelper.defaultConnectionWithForcedSimple().use {
            if (isPrepared) {
                it.sendPreparedStatement(query, emptyList())
            } else {
                it.sendQuery(query)
            }
        }.toList()

        assertEquals(1, result.size)
        assertEquals(1, result[0].rowsAffected)
        val rows = result[0].rows.toList()
        assertEquals(1, rows.size)
        Assertions.assertIterableEquals(expectedResult, rows.map { it.getAs<List<Int>>(0) }.first())
    }

    @Test
    fun `decode should return int list when simple querying postgresql int array`(): Unit = runBlocking {
        `decode should return int list when querying postgresql int array`(isPrepared = false)
    }

    @Test
    fun `decode should return int list when extended querying postgresql int array`(): Unit = runBlocking {
        `decode should return int list when querying postgresql int array`(isPrepared = true)
    }
}
