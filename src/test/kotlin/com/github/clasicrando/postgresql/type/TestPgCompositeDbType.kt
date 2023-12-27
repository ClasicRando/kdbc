package com.github.clasicrando.postgresql.type

import com.github.clasicrando.common.column.ColumnData
import com.github.clasicrando.common.column.DateTimeDbType
import com.github.clasicrando.common.column.FloatDbType
import com.github.clasicrando.common.column.IntDbType
import com.github.clasicrando.common.column.StringDbType
import com.github.clasicrando.common.datetime.DateTime
import io.mockk.impl.annotations.RelaxedMockK
import io.mockk.junit5.MockKExtension
import kotlinx.datetime.LocalDateTime
import kotlinx.datetime.UtcOffset
import kotlinx.datetime.asTimeZone
import org.junit.jupiter.api.extension.ExtendWith
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

@ExtendWith(MockKExtension::class)
class TestPgCompositeDbType {
    @RelaxedMockK
    lateinit var columnData: ColumnData

    data class CompositeType(
        val int: Int,
        val float: Float,
        val string: String,
        val dateTime: DateTime,
    )

    @Test
    fun `pgCompositeDbType should fail when not data class`() {
        assertFailsWith<IllegalArgumentException> {
            pgCompositeDbType<Int>(arrayOf())
        }
    }

    @Test
    fun `pgCompositeDbType should fail when empty array provided`() {
        assertFailsWith<IllegalArgumentException> {
            pgCompositeDbType<CompositeType>(arrayOf())
        }
    }

    @Test
    fun `pgCompositeDbType should fail when number of inner types does not match class parameters`() {
        assertFailsWith<IllegalArgumentException> {
            pgCompositeDbType<CompositeType>(arrayOf(IntDbType))
        }
    }

    /* TypeRegistry handles composite type encoding */
    @Test
    fun `encode should always fail`() {
        assertFailsWith<NotImplementedError> {
            pgCompositeDbType<CompositeType>(
                arrayOf(IntDbType, FloatDbType, StringDbType, DateTimeDbType)
            ).encode("")
        }
    }

    @Test
    fun `decode should return new instance when valid composite literal string`() {
        val compositeLiteral = "(1,2.56,\"CompositeType, Test\",\"2023-01-01 12:58:56.2335+01\")"
        val dbType = pgCompositeDbType<CompositeType>(
            arrayOf(IntDbType, FloatDbType, StringDbType, DateTimeDbType)
        )

        val result = dbType.decode(columnData, compositeLiteral)

        assertTrue(result is CompositeType)
        assertEquals(1, result.int)
        assertEquals(2.56F, result.float)
        assertEquals("CompositeType, Test", result.string)
        val dateTime = DateTime(
            LocalDateTime(
                year = 2023,
                monthNumber = 1,
                dayOfMonth = 1,
                hour = 12,
                minute = 58,
                second = 56,
                nanosecond = 233_500_000,
            ),
            UtcOffset(1).asTimeZone(),
        )
        assertEquals(dateTime, result.dateTime)
    }
}
