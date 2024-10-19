package io.github.clasicrando.kdbc.postgresql.column

import com.ionspin.kotlin.bignum.decimal.BigDecimal
import io.github.clasicrando.kdbc.core.datetime.DateTime
import io.github.clasicrando.kdbc.core.query.bind
import io.github.clasicrando.kdbc.core.query.fetchScalar
import io.github.clasicrando.kdbc.core.use
import io.github.clasicrando.kdbc.postgresql.PgConnectionHelper
import io.github.clasicrando.kdbc.postgresql.type.Bound
import io.github.clasicrando.kdbc.postgresql.type.DateRange
import io.github.clasicrando.kdbc.postgresql.type.Int4Range
import io.github.clasicrando.kdbc.postgresql.type.Int8Range
import io.github.clasicrando.kdbc.postgresql.type.JDateRange
import io.github.clasicrando.kdbc.postgresql.type.JNumRange
import io.github.clasicrando.kdbc.postgresql.type.JTsRange
import io.github.clasicrando.kdbc.postgresql.type.JTsTzRange
import io.github.clasicrando.kdbc.postgresql.type.NumRange
import io.github.clasicrando.kdbc.postgresql.type.TsRange
import io.github.clasicrando.kdbc.postgresql.type.TsTzRange
import io.github.clasicrando.kdbc.postgresql.type.toDateRange
import io.github.clasicrando.kdbc.postgresql.type.toIntRange
import io.github.clasicrando.kdbc.postgresql.type.toJDateRange
import io.github.clasicrando.kdbc.postgresql.type.toLongRange
import kotlinx.coroutines.runBlocking
import kotlinx.datetime.Instant
import kotlinx.datetime.LocalDate
import kotlinx.datetime.TimeZone
import kotlinx.datetime.UtcOffset
import kotlinx.datetime.atStartOfDayIn
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.MethodSource
import java.time.ZoneOffset
import java.util.stream.Stream
import kotlin.test.assertEquals

class TestRange {
    @ParameterizedTest
    @MethodSource("int4RangeValues")
    fun `encode should accept Int4Range when querying postgresql`(pair: Pair<String, Int4Range>): Unit = runBlocking {
        val (_, value) = pair
        val query = "SELECT $1 int4range_col;"

        PgConnectionHelper.defaultConnection().use { conn ->
            val range = conn.createPreparedQuery(query)
                .bind(value)
                .fetchScalar<Int4Range>()
            assertEquals(value.toIntRange(), range?.toIntRange())
        }
    }

    private suspend fun int4RangeDecodeTest(isPrepared: Boolean, value: Int4Range, typeName: String) {
        val query = "SELECT '${value.postgresqlLiteral}'::$typeName;"

        PgConnectionHelper.defaultConnectionWithForcedSimple().use { conn ->
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
    fun `encode should accept Int8Range when querying postgresql`(pair: Pair<String, Int8Range>): Unit = runBlocking {
        val (_, value) = pair
        val query = "SELECT $1 int8range_col;"

        PgConnectionHelper.defaultConnection().use { conn ->
            val range = conn.createPreparedQuery(query)
                .bind(value)
                .fetchScalar<Int8Range>()
            assertEquals(value.toLongRange(), range?.toLongRange())
        }
    }

    private suspend fun int8RangeDecodeTest(isPrepared: Boolean, value: Int8Range, typeName: String) {
        val query = "SELECT '${value.postgresqlLiteral}'::$typeName;"

        PgConnectionHelper.defaultConnectionWithForcedSimple().use { conn ->
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
    fun `encode should accept NumRange when querying postgresql`(pair: Pair<String, NumRange>): Unit = runBlocking {
        val (_, value) = pair
        val query = "SELECT $1 numrange_col;"

        PgConnectionHelper.defaultConnection().use { conn ->
            val range = conn.createPreparedQuery(query)
                .bind(value)
                .fetchScalar<NumRange>()
            assertEquals(value, range)
        }
    }

    private suspend fun numRangeDecodeTest(isPrepared: Boolean, value: NumRange, typeName: String) {
        val query = "SELECT '${value.postgresqlLiteral}'::$typeName numrange_col;"

        PgConnectionHelper.defaultConnectionWithForcedSimple().use { conn ->
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
    @MethodSource("jNumrangeValues")
    fun `encode should accept JNumRange when querying postgresql`(pair: Pair<String, JNumRange>): Unit = runBlocking {
        val (_, value) = pair
        val query = "SELECT $1 jnumrange_col;"

        PgConnectionHelper.defaultConnection().use { conn ->
            val range = conn.createPreparedQuery(query)
                .bind(value)
                .fetchScalar<JNumRange>()
            assertEquals(value, range)
        }
    }

    private suspend fun jNumRangeDecodeTest(isPrepared: Boolean, value: JNumRange, typeName: String) {
        val query = "SELECT '${value.postgresqlLiteral}'::$typeName jnumrange_col;"

        PgConnectionHelper.defaultConnectionWithForcedSimple().use { conn ->
            val range = if (isPrepared) {
                conn.createPreparedQuery(query)
            } else {
                conn.createQuery(query)
            }.fetchScalar<JNumRange>()
            assertEquals(value, range)
        }
    }

    @ParameterizedTest
    @MethodSource("jNumrangeValues")
    fun `decode should return JNumRange when simple querying postgresql range`(pair: Pair<String, JNumRange>): Unit = runBlocking {
        val (typeName, range) = pair
        jNumRangeDecodeTest(isPrepared = false, value = range, typeName)
    }

    @ParameterizedTest
    @MethodSource("jNumrangeValues")
    fun `decode should return JNumRange when extended querying postgresql range`(pair: Pair<String, JNumRange>): Unit = runBlocking {
        val (typeName, range) = pair
        jNumRangeDecodeTest(isPrepared = true, value = range, typeName)
    }

    @ParameterizedTest
    @MethodSource("tsrangeValues")
    fun `encode should accept TsRange when querying postgresql`(pair: Pair<String, TsRange>): Unit = runBlocking {
        val (_, value) = pair
        val query = "SELECT $1 tsrange_col;"

        PgConnectionHelper.defaultConnection().use { conn ->
            val range = conn.createPreparedQuery(query)
                .bind(value)
                .fetchScalar<TsRange>()
            assertEquals(value, range)
        }
    }

    private suspend fun tsRangeDecodeTest(isPrepared: Boolean, value: TsRange, typeName: String) {
        val query = "SELECT '${value.postgresqlLiteral}'::$typeName tsrange_col;"

        PgConnectionHelper.defaultConnectionWithForcedSimple().use { conn ->
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
    @MethodSource("jTsrangeValues")
    fun `encode should accept JTsRange when querying postgresql`(pair: Pair<String, JTsRange>): Unit = runBlocking {
        val (_, value) = pair
        val query = "SELECT $1 jtsrange_col;"

        PgConnectionHelper.defaultConnection().use { conn ->
            val range = conn.createPreparedQuery(query)
                .bind(value)
                .fetchScalar<JTsRange>()
            assertEquals(value, range)
        }
    }

    private suspend fun jTsRangeDecodeTest(isPrepared: Boolean, value: JTsRange, typeName: String) {
        val query = "SELECT '${value.postgresqlLiteral}'::$typeName jtsrange_col;"

        PgConnectionHelper.defaultConnectionWithForcedSimple().use { conn ->
            val range = if (isPrepared) {
                conn.createPreparedQuery(query)
            } else {
                conn.createQuery(query)
            }.fetchScalar<JTsRange>()
            assertEquals(value, range)
        }
    }

    @ParameterizedTest
    @MethodSource("jTsrangeValues")
    fun `decode should return JTsRange when simple querying postgresql range`(pair: Pair<String, JTsRange>): Unit = runBlocking {
        val (typeName, range) = pair
        jTsRangeDecodeTest(isPrepared = false, value = range, typeName)
    }

    @ParameterizedTest
    @MethodSource("jTsrangeValues")
    fun `decode should return JTsRange when extended querying postgresql range`(pair: Pair<String, JTsRange>): Unit = runBlocking {
        val (typeName, range) = pair
        jTsRangeDecodeTest(isPrepared = true, value = range, typeName)
    }

    @ParameterizedTest
    @MethodSource("tstzrangeValues")
    fun `encode should accept TsTzRange when querying postgresql`(pair: Pair<String, TsTzRange>): Unit = runBlocking {
        val (_, value) = pair
        val query = "SELECT $1 tstzrange_col;"

        PgConnectionHelper.defaultConnection().use { conn ->
            val range = conn.createPreparedQuery(query)
                .bind(value)
                .fetchScalar<TsTzRange>()
            assertEquals(value, range)
        }
    }

    private suspend fun tstzRangeDecodeTest(isPrepared: Boolean, value: TsTzRange, typeName: String) {
        val query = "SELECT '${value.postgresqlLiteral}'::$typeName tstzrange_col;"

        PgConnectionHelper.defaultConnectionWithForcedSimple().use { conn ->
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
    @MethodSource("jTstzrangeValues")
    fun `encode should accept JTsTzRange when querying postgresql`(pair: Pair<String, JTsTzRange>): Unit = runBlocking {
        val (_, value) = pair
        val query = "SELECT $1 jtstzrange_col;"

        PgConnectionHelper.defaultConnection().use { conn ->
            val range = conn.createPreparedQuery(query)
                .bind(value)
                .fetchScalar<JTsTzRange>()
            assertEquals(value, range)
        }
    }

    private suspend fun jTstzRangeDecodeTest(isPrepared: Boolean, value: JTsTzRange, typeName: String) {
        val query = "SELECT '${value.postgresqlLiteral}'::$typeName jtstzrange_col;"

        PgConnectionHelper.defaultConnectionWithForcedSimple().use { conn ->
            val range = if (isPrepared) {
                conn.createPreparedQuery(query)
            } else {
                conn.createQuery(query)
            }.fetchScalar<JTsTzRange>()
            assertEquals(value, range)
        }
    }

    @ParameterizedTest
    @MethodSource("jTstzrangeValues")
    fun `decode should return JTsTzRange when simple querying postgresql range`(pair: Pair<String, JTsTzRange>): Unit = runBlocking {
        val (typeName, range) = pair
        jTstzRangeDecodeTest(isPrepared = false, value = range, typeName)
    }

    @ParameterizedTest
    @MethodSource("jTstzrangeValues")
    fun `decode should return JTsTzRange when extended querying postgresql range`(pair: Pair<String, JTsTzRange>): Unit = runBlocking {
        val (typeName, range) = pair
        jTstzRangeDecodeTest(isPrepared = true, value = range, typeName)
    }

    @ParameterizedTest
    @MethodSource("daterangeValues")
    fun `encode should accept DateRange when querying postgresql`(pair: Pair<String, DateRange>): Unit = runBlocking {
        val (_, value) = pair
        val query = "SELECT $1 daterange_col;"

        PgConnectionHelper.defaultConnection().use { conn ->
            val range = conn.createPreparedQuery(query)
                .bind(value)
                .fetchScalar<DateRange>()
            assertEquals(value.toDateRange(), range?.toDateRange())
        }
    }

    private suspend fun dateRangeDecodeTest(isPrepared: Boolean, value: DateRange, typeName: String) {
        val query = "SELECT '${value.postgresqlLiteral}'::$typeName daterange_col;"

        PgConnectionHelper.defaultConnectionWithForcedSimple().use { conn ->
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

    @ParameterizedTest
    @MethodSource("jDaterangeValues")
    fun `encode should accept JDateRange when querying postgresql`(pair: Pair<String, JDateRange>): Unit = runBlocking {
        val (_, value) = pair
        val query = "SELECT $1 jdaterange_col;"

        PgConnectionHelper.defaultConnection().use { conn ->
            val range = conn.createPreparedQuery(query)
                .bind(value)
                .fetchScalar<JDateRange>()
            assertEquals(value.toJDateRange(), range?.toJDateRange())
        }
    }

    private suspend fun jDateRangeDecodeTest(isPrepared: Boolean, value: JDateRange, typeName: String) {
        val query = "SELECT '${value.postgresqlLiteral}'::$typeName jdaterange_col;"

        PgConnectionHelper.defaultConnectionWithForcedSimple().use { conn ->
            val range = if (isPrepared) {
                conn.createPreparedQuery(query)
            } else {
                conn.createQuery(query)
            }.fetchScalar<JDateRange>()
            assertEquals(value.toJDateRange(), range?.toJDateRange())
        }
    }

    @ParameterizedTest
    @MethodSource("jDaterangeValues")
    fun `decode should return JDateRange when simple querying postgresql range`(pair: Pair<String, JDateRange>): Unit = runBlocking {
        val (typeName, range) = pair
        jDateRangeDecodeTest(isPrepared = false, value = range, typeName)
    }

    @ParameterizedTest
    @MethodSource("jDaterangeValues")
    fun `decode should return JDateRange when extended querying postgresql range`(pair: Pair<String, JDateRange>): Unit = runBlocking {
        val (typeName, range) = pair
        jDateRangeDecodeTest(isPrepared = true, value = range, typeName)
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

        private val lowerBigDecimal = BigDecimal.fromLong(1L)
        private val upperBigDecimal = BigDecimal.fromLong(3L)
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

        private val lowerJBigDecimal = java.math.BigDecimal.valueOf(1L)
        private val upperJBigDecimal = java.math.BigDecimal.valueOf(3L)
        private const val JNUMRANGE_TYPE_NAME = "numrange"

        @JvmStatic
        fun jNumrangeValues(): Stream<Pair<String, JNumRange>> {
            return listOf(
                Bound.Included(lowerJBigDecimal) to Bound.Included(upperJBigDecimal),
                Bound.Included(lowerJBigDecimal) to Bound.Excluded(upperJBigDecimal),
                Bound.Included(lowerJBigDecimal) to Bound.Unbounded(),
                Bound.Excluded(lowerJBigDecimal) to Bound.Included(upperJBigDecimal),
                Bound.Excluded(lowerJBigDecimal) to Bound.Excluded(upperJBigDecimal),
                Bound.Excluded(lowerJBigDecimal) to Bound.Unbounded(),
                Bound.Unbounded<java.math.BigDecimal>() to Bound.Included(upperJBigDecimal),
                Bound.Unbounded<java.math.BigDecimal>() to Bound.Excluded(upperJBigDecimal),
                Bound.Unbounded<java.math.BigDecimal>() to Bound.Unbounded(),
            )
                .map { JNUMRANGE_TYPE_NAME to JNumRange(it.first, it.second) }
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

        private val lowerJTimestamp = java.time.LocalDate.of(2024, 1, 1).atStartOfDay()
        private val upperJTimestamp = java.time.LocalDate.of(2024, 2, 1).atStartOfDay()
        private const val JTSRANGE_TYPE_NAME = "tsrange"

        @JvmStatic
        fun jTsrangeValues(): Stream<Pair<String, JTsRange>> {
            return listOf(
                Bound.Included(lowerJTimestamp) to Bound.Included(upperJTimestamp),
                Bound.Included(lowerJTimestamp) to Bound.Excluded(upperJTimestamp),
                Bound.Included(lowerJTimestamp) to Bound.Unbounded(),
                Bound.Excluded(lowerJTimestamp) to Bound.Included(upperJTimestamp),
                Bound.Excluded(lowerJTimestamp) to Bound.Excluded(upperJTimestamp),
                Bound.Excluded(lowerJTimestamp) to Bound.Unbounded(),
                Bound.Unbounded<java.time.LocalDateTime>() to Bound.Included(upperJTimestamp),
                Bound.Unbounded<java.time.LocalDateTime>() to Bound.Excluded(upperJTimestamp),
                Bound.Unbounded<java.time.LocalDateTime>() to Bound.Unbounded(),
            )
                .map { JTSRANGE_TYPE_NAME to JTsRange(it.first, it.second) }
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

        private val lowerJTimestampTz = java.time.OffsetDateTime.of(
            java.time.LocalDate.of(2024, 1, 1).atStartOfDay(),
            ZoneOffset.UTC
        )
        private val upperJTimestampTz = java.time.OffsetDateTime.of(
            java.time.LocalDate.of(2024, 2, 1).atStartOfDay(),
            ZoneOffset.UTC
        )
        private const val JTSTZRANGE_TYPE_NAME = "tstzrange"

        @JvmStatic
        fun jTstzrangeValues(): Stream<Pair<String, JTsTzRange>> {
            return listOf(
                Bound.Included(lowerJTimestampTz) to Bound.Included(upperJTimestampTz),
                Bound.Included(lowerJTimestampTz) to Bound.Excluded(upperJTimestampTz),
                Bound.Included(lowerJTimestampTz) to Bound.Unbounded(),
                Bound.Excluded(lowerJTimestampTz) to Bound.Included(upperJTimestampTz),
                Bound.Excluded(lowerJTimestampTz) to Bound.Excluded(upperJTimestampTz),
                Bound.Excluded(lowerJTimestampTz) to Bound.Unbounded(),
                Bound.Unbounded<java.time.OffsetDateTime>() to Bound.Included(upperJTimestampTz),
                Bound.Unbounded<java.time.OffsetDateTime>() to Bound.Excluded(upperJTimestampTz),
                Bound.Unbounded<java.time.OffsetDateTime>() to Bound.Unbounded(),
            )
                .map { JTSTZRANGE_TYPE_NAME to JTsTzRange(it.first, it.second) }
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

        private val lowerJDate = java.time.LocalDate.of(2024, 1, 1)
        private val upperJDate = java.time.LocalDate.of(2024, 2, 1)
        private const val JDATERANGE_TYPE_NAME = "daterange"

        @JvmStatic
        fun jDaterangeValues(): Stream<Pair<String, JDateRange>> {
            return listOf(
                Bound.Included(lowerJDate) to Bound.Included(upperJDate),
                Bound.Included(lowerJDate) to Bound.Excluded(upperJDate),
                Bound.Included(lowerJDate) to Bound.Unbounded(),
                Bound.Excluded(lowerJDate) to Bound.Included(upperJDate),
                Bound.Excluded(lowerJDate) to Bound.Excluded(upperJDate),
                Bound.Excluded(lowerJDate) to Bound.Unbounded(),
                Bound.Unbounded<java.time.LocalDate>() to Bound.Included(upperJDate),
                Bound.Unbounded<java.time.LocalDate>() to Bound.Excluded(upperJDate),
                Bound.Unbounded<java.time.LocalDate>() to Bound.Unbounded(),
            )
                .map { JDATERANGE_TYPE_NAME to JDateRange(it.first, it.second) }
                .stream()
        }
    }
}
