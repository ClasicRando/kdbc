package io.github.clasicrando.kdbc.postgresql.column

import io.github.clasicrando.kdbc.core.connection.use
import io.github.clasicrando.kdbc.core.datetime.DateTime
import io.github.clasicrando.kdbc.core.query.bind
import io.github.clasicrando.kdbc.core.query.fetchScalar
import io.github.clasicrando.kdbc.postgresql.PgConnectionHelper
import io.github.clasicrando.kdbc.postgresql.type.Bound
import io.github.clasicrando.kdbc.postgresql.type.toDateRange
import io.github.clasicrando.kdbc.postgresql.type.toIntRange
import io.github.clasicrando.kdbc.postgresql.type.toLongRange
import kotlinx.coroutines.runBlocking
import kotlinx.datetime.Instant
import kotlinx.datetime.LocalDate
import kotlinx.datetime.TimeZone
import kotlinx.datetime.UtcOffset
import kotlinx.datetime.atStartOfDayIn
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.MethodSource
import java.math.BigDecimal
import java.util.stream.Stream
import kotlin.test.assertEquals

class TestRange {
    @ParameterizedTest
    @MethodSource("int4RangeValues")
    fun `encode should accept Int4Range when querying postgresql`(pair: Pair<String, Int4Range>) = runBlocking {
        val (_, value) = pair
        val query = "SELECT $1 int4range_col;"

        PgConnectionHelper.defaultSuspendingConnection().use { conn ->
            val range = conn.createPreparedQuery(query)
                .bind(value)
                .fetchScalar<Int4Range>()
            assertEquals(value.toIntRange(), range?.toIntRange())
        }
    }

    private suspend fun int4RangeDecodeTest(isPrepared: Boolean, value: Int4Range, typeName: String) {
        val query = "SELECT '${value.postgresqlLiteral}'::$typeName;"

        PgConnectionHelper.defaultSuspendingConnectionWithForcedSimple().use { conn ->
            val range = if (isPrepared) {
                conn.createPreparedQuery(query)
            } else {
                conn.createQuery(query)
            }.fetchScalar<Int4Range>()
            assertEquals(value.toIntRange(), range?.toIntRange())
        }
    }

    @ParameterizedTest
    @MethodSource("int4RangeValues")
    fun `decode should return Int4Range when simple querying postgresql range`(pair: Pair<String, Int4Range>): Unit = runBlocking {
        val (typeName, range) = pair
        int4RangeDecodeTest(isPrepared = false, value = range, typeName)
    }

    @ParameterizedTest
    @MethodSource("int4RangeValues")
    fun `decode should return Int4Range when extended querying postgresql range`(pair: Pair<String, Int4Range>): Unit = runBlocking {
        val (typeName, range) = pair
        int4RangeDecodeTest(isPrepared = true, value = range, typeName)
    }

    @ParameterizedTest
    @MethodSource("int8rangeValues")
    fun `encode should accept Int8Range when querying postgresql`(pair: Pair<String, Int8Range>) = runBlocking {
        val (_, value) = pair
        val query = "SELECT $1 int8range_col;"

        PgConnectionHelper.defaultSuspendingConnection().use { conn ->
            val range = conn.createPreparedQuery(query)
                .bind(value)
                .fetchScalar<Int8Range>()
            assertEquals(value.toLongRange(), range?.toLongRange())
        }
    }

    private suspend fun int8RangeDecodeTest(isPrepared: Boolean, value: Int8Range, typeName: String) {
        val query = "SELECT '${value.postgresqlLiteral}'::$typeName;"

        PgConnectionHelper.defaultSuspendingConnectionWithForcedSimple().use { conn ->
            val range = if (isPrepared) {
                conn.createPreparedQuery(query)
            } else {
                conn.createQuery(query)
            }.fetchScalar<Int8Range>()
            assertEquals(value.toLongRange(), range?.toLongRange())
        }
    }

    @ParameterizedTest
    @MethodSource("int8rangeValues")
    fun `decode should return Int8Range when simple querying postgresql range`(pair: Pair<String, Int8Range>): Unit = runBlocking {
        val (typeName, range) = pair
        int8RangeDecodeTest(isPrepared = false, value = range, typeName)
    }

    @ParameterizedTest
    @MethodSource("int8rangeValues")
    fun `decode should return Int8Range when extended querying postgresql range`(pair: Pair<String, Int8Range>): Unit = runBlocking {
        val (typeName, range) = pair
        int8RangeDecodeTest(isPrepared = true, value = range, typeName)
    }

    @ParameterizedTest
    @MethodSource("numrangeValues")
    fun `encode should accept NumRange when querying postgresql`(pair: Pair<String, NumRange>) = runBlocking {
        val (_, value) = pair
        val query = "SELECT $1 numrange_col;"

        PgConnectionHelper.defaultSuspendingConnection().use { conn ->
            val range = conn.createPreparedQuery(query)
                .bind(value)
                .fetchScalar<NumRange>()
            assertEquals(value, range)
        }
    }

    private suspend fun numRangeDecodeTest(isPrepared: Boolean, value: NumRange, typeName: String) {
        val query = "SELECT '${value.postgresqlLiteral}'::$typeName;"

        PgConnectionHelper.defaultSuspendingConnectionWithForcedSimple().use { conn ->
            val range = if (isPrepared) {
                conn.createPreparedQuery(query)
            } else {
                conn.createQuery(query)
            }.fetchScalar<NumRange>()
            assertEquals(value, range)
        }
    }

    @ParameterizedTest
    @MethodSource("numrangeValues")
    fun `decode should return NumRange when simple querying postgresql range`(pair: Pair<String, NumRange>): Unit = runBlocking {
        val (typeName, range) = pair
        numRangeDecodeTest(isPrepared = false, value = range, typeName)
    }

    @ParameterizedTest
    @MethodSource("numrangeValues")
    fun `decode should return NumRange when extended querying postgresql range`(pair: Pair<String, NumRange>): Unit = runBlocking {
        val (typeName, range) = pair
        numRangeDecodeTest(isPrepared = true, value = range, typeName)
    }

    @ParameterizedTest
    @MethodSource("tsrangeValues")
    fun `encode should accept TsRange when querying postgresql`(pair: Pair<String, TsRange>) = runBlocking {
        val (_, value) = pair
        val query = "SELECT $1 tsrange_col;"

        PgConnectionHelper.defaultSuspendingConnection().use { conn ->
            val range = conn.createPreparedQuery(query)
                .bind(value)
                .fetchScalar<TsRange>()
            assertEquals(value, range)
        }
    }

    private suspend fun tsRangeDecodeTest(isPrepared: Boolean, value: TsRange, typeName: String) {
        val query = "SELECT '${value.postgresqlLiteral}'::$typeName;"

        PgConnectionHelper.defaultSuspendingConnectionWithForcedSimple().use { conn ->
            val range = if (isPrepared) {
                conn.createPreparedQuery(query)
            } else {
                conn.createQuery(query)
            }.fetchScalar<TsRange>()
            assertEquals(value, range)
        }
    }

    @ParameterizedTest
    @MethodSource("tsrangeValues")
    fun `decode should return TsRange when simple querying postgresql range`(pair: Pair<String, TsRange>): Unit = runBlocking {
        val (typeName, range) = pair
        tsRangeDecodeTest(isPrepared = false, value = range, typeName)
    }

    @ParameterizedTest
    @MethodSource("tsrangeValues")
    fun `decode should return TsRange when extended querying postgresql range`(pair: Pair<String, TsRange>): Unit = runBlocking {
        val (typeName, range) = pair
        tsRangeDecodeTest(isPrepared = true, value = range, typeName)
    }

    @ParameterizedTest
    @MethodSource("tstzrangeValues")
    fun `encode should accept TsTzRange when querying postgresql`(pair: Pair<String, TsTzRange>) = runBlocking {
        val (_, value) = pair
        val query = "SELECT $1 tstzrange_col;"

        PgConnectionHelper.defaultSuspendingConnection().use { conn ->
            val range = conn.createPreparedQuery(query)
                .bind(value)
                .fetchScalar<TsTzRange>()
            assertEquals(value, range)
        }
    }

    private suspend fun tstzRangeDecodeTest(isPrepared: Boolean, value: TsTzRange, typeName: String) {
        val query = "SELECT '${value.postgresqlLiteral}'::$typeName;"

        PgConnectionHelper.defaultSuspendingConnectionWithForcedSimple().use { conn ->
            val range = if (isPrepared) {
                conn.createPreparedQuery(query)
            } else {
                conn.createQuery(query)
            }.fetchScalar<TsTzRange>()
            assertEquals(value, range)
        }
    }

    @ParameterizedTest
    @MethodSource("tstzrangeValues")
    fun `decode should return TsTzRange when simple querying postgresql range`(pair: Pair<String, TsTzRange>): Unit = runBlocking {
        val (typeName, range) = pair
        tstzRangeDecodeTest(isPrepared = false, value = range, typeName)
    }

    @ParameterizedTest
    @MethodSource("tstzrangeValues")
    fun `decode should return TsTzRange when extended querying postgresql range`(pair: Pair<String, TsTzRange>): Unit = runBlocking {
        val (typeName, range) = pair
        tstzRangeDecodeTest(isPrepared = true, value = range, typeName)
    }

    @ParameterizedTest
    @MethodSource("daterangeValues")
    fun `encode should accept DateRange when querying postgresql`(pair: Pair<String, DateRange>) = runBlocking {
        val (_, value) = pair
        val query = "SELECT $1 daterange_col;"

        PgConnectionHelper.defaultSuspendingConnection().use { conn ->
            val range = conn.createPreparedQuery(query)
                .bind(value)
                .fetchScalar<DateRange>()
            assertEquals(value.toDateRange(), range?.toDateRange())
        }
    }

    private suspend fun dateRangeDecodeTest(isPrepared: Boolean, value: DateRange, typeName: String) {
        val query = "SELECT '${value.postgresqlLiteral}'::$typeName;"

        PgConnectionHelper.defaultSuspendingConnectionWithForcedSimple().use { conn ->
            val range = if (isPrepared) {
                conn.createPreparedQuery(query)
            } else {
                conn.createQuery(query)
            }.fetchScalar<DateRange>()
            assertEquals(value.toDateRange(), range?.toDateRange())
        }
    }

    @ParameterizedTest
    @MethodSource("daterangeValues")
    fun `decode should return DateRange when simple querying postgresql range`(pair: Pair<String, DateRange>): Unit = runBlocking {
        val (typeName, range) = pair
        dateRangeDecodeTest(isPrepared = false, value = range, typeName)
    }

    @ParameterizedTest
    @MethodSource("daterangeValues")
    fun `decode should return DateRange when extended querying postgresql range`(pair: Pair<String, DateRange>): Unit = runBlocking {
        val (typeName, range) = pair
        dateRangeDecodeTest(isPrepared = true, value = range, typeName)
    }

    companion object {
        private const val LOWER_INT = 1
        private const val UPPER_INT = 3
        private const val INT4RANGE_TYPE_NAME = "int4range"

        @JvmStatic
        fun int4RangeValues(): Stream<Pair<String, Int4Range>> {
            return listOf(
                Int4Range(Bound.Included(LOWER_INT), Bound.Included(UPPER_INT)),
                Int4Range(Bound.Included(LOWER_INT), Bound.Excluded(UPPER_INT)),
                Int4Range(Bound.Included(LOWER_INT), Bound.Unbounded()),
                Int4Range(Bound.Excluded(LOWER_INT), Bound.Included(UPPER_INT)),
                Int4Range(Bound.Excluded(LOWER_INT), Bound.Excluded(UPPER_INT)),
                Int4Range(Bound.Excluded(LOWER_INT), Bound.Unbounded()),
                Int4Range(Bound.Unbounded(), Bound.Included(3)),
                Int4Range(Bound.Unbounded(), Bound.Excluded(3)),
                Int4Range(Bound.Unbounded(), Bound.Unbounded()),
            )
                .map { INT4RANGE_TYPE_NAME to it }
                .stream()
        }

        private const val LOWER_LONG = 1L
        private const val UPPER_LONG = 3L
        private const val INT8RANGE_TYPE_NAME = "int8range"

        @JvmStatic
        fun int8rangeValues(): Stream<Pair<String, Int8Range>> {
            return listOf(
                Int8Range(Bound.Included(LOWER_LONG), Bound.Included(UPPER_LONG)),
                Int8Range(Bound.Included(LOWER_LONG), Bound.Excluded(UPPER_LONG)),
                Int8Range(Bound.Included(LOWER_LONG), Bound.Unbounded()),
                Int8Range(Bound.Excluded(LOWER_LONG), Bound.Included(UPPER_LONG)),
                Int8Range(Bound.Excluded(LOWER_LONG), Bound.Excluded(UPPER_LONG)),
                Int8Range(Bound.Excluded(LOWER_LONG), Bound.Unbounded()),
                Int8Range(Bound.Unbounded(), Bound.Included(UPPER_LONG)),
                Int8Range(Bound.Unbounded(), Bound.Excluded(UPPER_LONG)),
                Int8Range(Bound.Unbounded(), Bound.Unbounded()),
            )
                .map { INT8RANGE_TYPE_NAME to it }
                .stream()
        }

        private val lowerBigDecimal = BigDecimal.valueOf(1L)
        private val upperBigDecimal = BigDecimal.valueOf(3L)
        private const val NUMRANGE_TYPE_NAME = "numrange"

        @JvmStatic
        fun numrangeValues(): Stream<Pair<String, NumRange>> {
            return listOf(
                Bound.Included(lowerBigDecimal) to Bound.Included(upperBigDecimal),
                Bound.Included(lowerBigDecimal) to Bound.Excluded(upperBigDecimal),
                Bound.Included(lowerBigDecimal) to Bound.Unbounded(),
                Bound.Excluded(lowerBigDecimal) to Bound.Included(upperBigDecimal),
                Bound.Excluded(lowerBigDecimal) to Bound.Excluded(upperBigDecimal),
                Bound.Excluded(lowerBigDecimal) to Bound.Unbounded(),
                Bound.Unbounded<BigDecimal>() to Bound.Included(upperBigDecimal),
                Bound.Unbounded<BigDecimal>() to Bound.Excluded(upperBigDecimal),
                Bound.Unbounded<BigDecimal>() to Bound.Unbounded(),
            )
                .map { NUMRANGE_TYPE_NAME to NumRange(it.first, it.second) }
                .stream()
        }

        private val lowerTimestamp = LocalDate(2024, 1, 1).atStartOfDayIn(TimeZone.UTC)
        private val upperTimestamp = LocalDate(2024, 2, 1).atStartOfDayIn(TimeZone.UTC)
        private const val TSRANGE_TYPE_NAME = "tsrange"

        @JvmStatic
        fun tsrangeValues(): Stream<Pair<String, TsRange>> {
            return listOf(
                Bound.Included(lowerTimestamp) to Bound.Included(upperTimestamp),
                Bound.Included(lowerTimestamp) to Bound.Excluded(upperTimestamp),
                Bound.Included(lowerTimestamp) to Bound.Unbounded(),
                Bound.Excluded(lowerTimestamp) to Bound.Included(upperTimestamp),
                Bound.Excluded(lowerTimestamp) to Bound.Excluded(upperTimestamp),
                Bound.Excluded(lowerTimestamp) to Bound.Unbounded(),
                Bound.Unbounded<Instant>() to Bound.Included(upperTimestamp),
                Bound.Unbounded<Instant>() to Bound.Excluded(upperTimestamp),
                Bound.Unbounded<Instant>() to Bound.Unbounded(),
            )
                .map { TSRANGE_TYPE_NAME to TsRange(it.first, it.second) }
                .stream()
        }

        private val lowerTimestampTz = DateTime(
            LocalDate(2024, 1, 1).atStartOfDayIn(TimeZone.UTC),
            UtcOffset.ZERO
        )
        private val upperTimestampTz = DateTime(
            LocalDate(2024, 2, 1).atStartOfDayIn(TimeZone.UTC),
            UtcOffset.ZERO
        )
        private const val TSTZRANGE_TYPE_NAME = "tstzrange"

        @JvmStatic
        fun tstzrangeValues(): Stream<Pair<String, TsTzRange>> {
            return listOf(
                Bound.Included(lowerTimestampTz) to Bound.Included(upperTimestampTz),
                Bound.Included(lowerTimestampTz) to Bound.Excluded(upperTimestampTz),
                Bound.Included(lowerTimestampTz) to Bound.Unbounded(),
                Bound.Excluded(lowerTimestampTz) to Bound.Included(upperTimestampTz),
                Bound.Excluded(lowerTimestampTz) to Bound.Excluded(upperTimestampTz),
                Bound.Excluded(lowerTimestampTz) to Bound.Unbounded(),
                Bound.Unbounded<DateTime>() to Bound.Included(upperTimestampTz),
                Bound.Unbounded<DateTime>() to Bound.Excluded(upperTimestampTz),
                Bound.Unbounded<DateTime>() to Bound.Unbounded(),
            )
                .map { TSTZRANGE_TYPE_NAME to TsTzRange(it.first, it.second) }
                .stream()
        }

        private val lowerDate = LocalDate(2024, 1, 1)
        private val upperDate = LocalDate(2024, 2, 1)
        private const val DATERANGE_TYPE_NAME = "daterange"

        @JvmStatic
        fun daterangeValues(): Stream<Pair<String, DateRange>> {
            return listOf(
                Bound.Included(lowerDate) to Bound.Included(upperDate),
                Bound.Included(lowerDate) to Bound.Excluded(upperDate),
                Bound.Included(lowerDate) to Bound.Unbounded(),
                Bound.Excluded(lowerDate) to Bound.Included(upperDate),
                Bound.Excluded(lowerDate) to Bound.Excluded(upperDate),
                Bound.Excluded(lowerDate) to Bound.Unbounded(),
                Bound.Unbounded<LocalDate>() to Bound.Included(upperDate),
                Bound.Unbounded<LocalDate>() to Bound.Excluded(upperDate),
                Bound.Unbounded<LocalDate>() to Bound.Unbounded(),
            )
                .map { DATERANGE_TYPE_NAME to DateRange(it.first, it.second) }
                .stream()
        }
    }
}
