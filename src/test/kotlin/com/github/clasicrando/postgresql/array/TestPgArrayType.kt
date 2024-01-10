package com.github.clasicrando.postgresql.array

import com.github.clasicrando.common.column.ColumnInfo
import com.github.clasicrando.common.column.ColumnDecodeError
import com.github.clasicrando.common.column.DbType
import com.github.clasicrando.common.column.IntDbType
import com.github.clasicrando.common.column.LocalDateTimeDbType
import com.github.clasicrando.common.column.StringDbType
import io.mockk.every
import io.mockk.mockk
import kotlinx.datetime.LocalDateTime
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.MethodSource
import java.util.stream.Stream
import kotlin.test.Test
import kotlin.test.assertTrue

class TestPgArrayType {
    @ParameterizedTest
    @MethodSource("decodeValues")
    fun `decode should return decoded value when valid array literal`(
        triple: Triple<String, DbType, List<Any>>,
    ) {
        val (literal, dbType, expected) = triple
        val type = PgArrayType(dbType)
        val columnType = mockk<ColumnInfo>()

        val result = type.decode(columnType, literal.toByteArray(), Charsets.UTF_8)

        assertTrue(result is List<*>)
        Assertions.assertIterableEquals(expected, result)
    }

    @Test
    fun `decode should throw a column decode error when literal is not wrapped by curl braces`() {
        val literal = "1,2,3,4"
        val type = PgArrayType(IntDbType)
        val columnType = mockk<ColumnInfo>()
        every { columnType.dataType } returns 1
        every { columnType.name } returns "Test"

        assertThrows<ColumnDecodeError> {
            type.decode(columnType, literal.toByteArray(), Charsets.UTF_8)
        }
    }

    companion object {
        @JvmStatic
        fun decodeValues(): Stream<Triple<String, DbType, List<Any?>>> {
            return Stream.of(
                Triple("{1,2,3,4}", IntDbType, listOf(1, 2, 3, 4)),
                Triple(
                    "{Test,NULL,Also a test}",
                    StringDbType,
                    listOf("Test", null, "Also a test"),
                ),
                Triple(
                    "{2023-01-01 22:02:59}",
                    LocalDateTimeDbType,
                    listOf(LocalDateTime(2023, 1, 1, 22, 2, 59)),
                ),
            )
        }
    }
}
