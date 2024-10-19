package io.github.clasicrando.kdbc.postgresql.column

import io.github.clasicrando.kdbc.core.DEFAULT_KDBC_TEST_TIMEOUT
import io.github.clasicrando.kdbc.core.column.ColumnDecodeError
import io.github.clasicrando.kdbc.core.query.RowParser
import io.github.clasicrando.kdbc.core.query.bind
import io.github.clasicrando.kdbc.core.query.fetchAll
import io.github.clasicrando.kdbc.core.query.fetchScalar
import io.github.clasicrando.kdbc.core.result.DataRow
import io.github.clasicrando.kdbc.core.result.getAsNonNull
import io.github.clasicrando.kdbc.core.use
import io.github.clasicrando.kdbc.postgresql.PgConnectionHelper
import io.github.clasicrando.kdbc.postgresql.type.ArrayTypeDescription
import io.github.clasicrando.kdbc.postgresql.type.InstantTypeDescription
import io.github.clasicrando.kdbc.postgresql.type.IntTypeDescription
import io.github.clasicrando.kdbc.postgresql.type.PgType
import io.github.clasicrando.kdbc.postgresql.type.VarcharTypeDescription
import kotlinx.coroutines.runBlocking
import kotlinx.datetime.Instant
import kotlinx.datetime.LocalDateTime
import kotlinx.datetime.TimeZone
import kotlinx.datetime.toInstant
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Timeout
import org.junit.jupiter.api.assertThrows
import kotlin.test.Test

internal object IntArrayTypeDescription : ArrayTypeDescription<Int>(
    pgType = PgType.Int4Array,
    innerType = IntTypeDescription,
)

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

        val description = object : ArrayTypeDescription<String>(
            pgType = PgType.VarcharArray,
            innerType = VarcharTypeDescription,
        ) {
            override fun isCompatible(dbType: PgType): Boolean {
                return dbType == PgType.TextArray
                        || dbType == PgType.VarcharArray
                        || dbType == PgType.XmlArray
                        || dbType == PgType.NameArray
                        || dbType == PgType.BpcharArray
            }
        }
        val result = description.decode(pgValue)

        Assertions.assertIterableEquals(listOf("Test", null, "Also a test"), result)
    }

    @Test
    fun `decode should return decoded value when valid array literal 3`() {
        val literal = "{2023-01-01 22:02:59}"
        val pgValue = PgValue.Text(literal, fieldDescription(PgType.TimestampArray))

        val description = object : ArrayTypeDescription<Instant>(
            pgType = PgType.TimestampArray,
            innerType = InstantTypeDescription,
        ) {}
        val result = description.decode(pgValue)

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
    fun `encode should accept int list when querying postgresql`(): Unit = runBlocking {
        val values = listOf(1, 2, 3, 4)
        val query = "SELECT x array_values FROM UNNEST($1) x"

        PgConnectionHelper.defaultConnection().use { conn ->
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

        PgConnectionHelper.defaultConnectionWithForcedSimple().use { conn ->
            val ints = if (isPrepared) {
                conn.createPreparedQuery(query)
            } else {
                conn.createQuery(query)
            }.fetchScalar<List<Int>>()
            Assertions.assertIterableEquals(expectedResult, ints)
        }
    }

    @Test
    @Timeout(value = DEFAULT_KDBC_TEST_TIMEOUT)
    fun `decode should return int list when simple querying postgresql int array`(): Unit = runBlocking {
        decodeTest(isPrepared = false)
    }

    @Test
    @Timeout(value = DEFAULT_KDBC_TEST_TIMEOUT)
    fun `decode should return int list when extended querying postgresql int array`(): Unit = runBlocking {
        decodeTest(isPrepared = true)
    }
}
