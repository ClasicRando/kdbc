package com.github.clasicrando.postgresql.array

import com.github.clasicrando.common.column.ColumnDecodeError
import com.github.clasicrando.postgresql.column.PgType
import com.github.clasicrando.postgresql.column.PgValue
import com.github.clasicrando.postgresql.column.arrayTypeDecoder
import com.github.clasicrando.postgresql.column.intTypeDecoder
import com.github.clasicrando.postgresql.column.localDateTimeTypeDecoder
import com.github.clasicrando.postgresql.column.stringTypeDecoder
import com.github.clasicrando.postgresql.row.PgColumnDescription
import kotlinx.datetime.LocalDateTime
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
}
