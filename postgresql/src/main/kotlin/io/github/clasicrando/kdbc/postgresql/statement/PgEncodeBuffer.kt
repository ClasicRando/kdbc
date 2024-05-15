package io.github.clasicrando.kdbc.postgresql.statement

import io.github.clasicrando.kdbc.core.AutoRelease
import io.github.clasicrando.kdbc.core.buffer.ByteListWriteBuffer
import io.github.clasicrando.kdbc.core.buffer.ByteWriteBuffer
import io.github.clasicrando.kdbc.core.buffer.writeLengthPrefixed
import io.github.clasicrando.kdbc.core.datetime.DateTime
import io.github.clasicrando.kdbc.postgresql.column.BigIntArrayTypeDescription
import io.github.clasicrando.kdbc.postgresql.column.BigIntTypeDescription
import io.github.clasicrando.kdbc.postgresql.column.BoolArrayTypeDescription
import io.github.clasicrando.kdbc.postgresql.column.BoolTypeDescription
import io.github.clasicrando.kdbc.postgresql.column.BoxArrayTypeDescription
import io.github.clasicrando.kdbc.postgresql.column.BoxTypeDescription
import io.github.clasicrando.kdbc.postgresql.column.BpcharArrayTypeDescription
import io.github.clasicrando.kdbc.postgresql.column.ByteaArrayTypeDescription
import io.github.clasicrando.kdbc.postgresql.column.ByteaTypeDescription
import io.github.clasicrando.kdbc.postgresql.column.CharArrayTypeDescription
import io.github.clasicrando.kdbc.postgresql.column.CharTypeDescription
import io.github.clasicrando.kdbc.postgresql.column.CidrArrayTypeDescription
import io.github.clasicrando.kdbc.postgresql.column.CidrTypeDescription
import io.github.clasicrando.kdbc.postgresql.column.CircleArrayTypeDescription
import io.github.clasicrando.kdbc.postgresql.column.CircleTypeDescription
import io.github.clasicrando.kdbc.postgresql.column.DateArrayTypeDescription
import io.github.clasicrando.kdbc.postgresql.column.DateRange
import io.github.clasicrando.kdbc.postgresql.column.DateRangeArrayTypeDescription
import io.github.clasicrando.kdbc.postgresql.column.DateRangeTypeDescription
import io.github.clasicrando.kdbc.postgresql.column.DateTypeDescription
import io.github.clasicrando.kdbc.postgresql.column.DoublePrecisionArrayTypeDescription
import io.github.clasicrando.kdbc.postgresql.column.DoublePrecisionTypeDescription
import io.github.clasicrando.kdbc.postgresql.column.InetArrayTypeDescription
import io.github.clasicrando.kdbc.postgresql.column.InetTypeDescription
import io.github.clasicrando.kdbc.postgresql.column.Int4Range
import io.github.clasicrando.kdbc.postgresql.column.Int4RangeArrayTypeDescription
import io.github.clasicrando.kdbc.postgresql.column.Int4RangeTypeDescription
import io.github.clasicrando.kdbc.postgresql.column.Int8Range
import io.github.clasicrando.kdbc.postgresql.column.Int8RangeArrayTypeDescription
import io.github.clasicrando.kdbc.postgresql.column.Int8RangeTypeDescription
import io.github.clasicrando.kdbc.postgresql.column.IntArrayTypeDescription
import io.github.clasicrando.kdbc.postgresql.column.IntTypeDescription
import io.github.clasicrando.kdbc.postgresql.column.IntervalArrayTypeDescription
import io.github.clasicrando.kdbc.postgresql.column.IntervalTypeDescription
import io.github.clasicrando.kdbc.postgresql.column.JsonArrayTypeDescription
import io.github.clasicrando.kdbc.postgresql.column.JsonTypeDescription
import io.github.clasicrando.kdbc.postgresql.column.JsonbArrayTypeDescription
import io.github.clasicrando.kdbc.postgresql.column.JsonbTypeDescription
import io.github.clasicrando.kdbc.postgresql.column.LineArrayTypeDescription
import io.github.clasicrando.kdbc.postgresql.column.LineSegmentArrayTypeDescription
import io.github.clasicrando.kdbc.postgresql.column.LineSegmentTypeDescription
import io.github.clasicrando.kdbc.postgresql.column.LineTypeDescription
import io.github.clasicrando.kdbc.postgresql.column.MacAddress8ArrayTypeDescription
import io.github.clasicrando.kdbc.postgresql.column.MacAddress8TypeDescription
import io.github.clasicrando.kdbc.postgresql.column.MacAddressArrayTypeDescription
import io.github.clasicrando.kdbc.postgresql.column.MacAddressTypeDescription
import io.github.clasicrando.kdbc.postgresql.column.MoneyArrayTypeDescription
import io.github.clasicrando.kdbc.postgresql.column.MoneyTypeDescription
import io.github.clasicrando.kdbc.postgresql.column.NameArrayTypeDescription
import io.github.clasicrando.kdbc.postgresql.column.NumRange
import io.github.clasicrando.kdbc.postgresql.column.NumRangeArrayTypeDescription
import io.github.clasicrando.kdbc.postgresql.column.NumRangeTypeDescription
import io.github.clasicrando.kdbc.postgresql.column.NumericArrayTypeDescription
import io.github.clasicrando.kdbc.postgresql.column.NumericTypeDescription
import io.github.clasicrando.kdbc.postgresql.column.PathArrayTypeDescription
import io.github.clasicrando.kdbc.postgresql.column.PathTypeDescription
import io.github.clasicrando.kdbc.postgresql.column.PgColumnDescription
import io.github.clasicrando.kdbc.postgresql.column.PgType
import io.github.clasicrando.kdbc.postgresql.column.PgTypeCache
import io.github.clasicrando.kdbc.postgresql.column.PointArrayTypeDescription
import io.github.clasicrando.kdbc.postgresql.column.PointTypeDescription
import io.github.clasicrando.kdbc.postgresql.column.PolygonArrayTypeDescription
import io.github.clasicrando.kdbc.postgresql.column.PolygonTypeDescription
import io.github.clasicrando.kdbc.postgresql.column.RealArrayTypeDescription
import io.github.clasicrando.kdbc.postgresql.column.RealTypeDescription
import io.github.clasicrando.kdbc.postgresql.column.SmallIntArrayTypeDescription
import io.github.clasicrando.kdbc.postgresql.column.SmallIntTypeDescription
import io.github.clasicrando.kdbc.postgresql.column.TextArrayTypeDescription
import io.github.clasicrando.kdbc.postgresql.column.TimeArrayTypeDescription
import io.github.clasicrando.kdbc.postgresql.column.TimeTypeDescription
import io.github.clasicrando.kdbc.postgresql.column.TimeTzArrayTypeDescription
import io.github.clasicrando.kdbc.postgresql.column.TimeTzTypeDescription
import io.github.clasicrando.kdbc.postgresql.column.TimestampArrayTypeDescription
import io.github.clasicrando.kdbc.postgresql.column.TimestampTypeDescription
import io.github.clasicrando.kdbc.postgresql.column.TimestampTzArrayTypeDescription
import io.github.clasicrando.kdbc.postgresql.column.TimestampTzTypeDescription
import io.github.clasicrando.kdbc.postgresql.column.TsRange
import io.github.clasicrando.kdbc.postgresql.column.TsRangeArrayTypeDescription
import io.github.clasicrando.kdbc.postgresql.column.TsRangeTypeDescription
import io.github.clasicrando.kdbc.postgresql.column.TsTzRange
import io.github.clasicrando.kdbc.postgresql.column.TsTzRangeArrayTypeDescription
import io.github.clasicrando.kdbc.postgresql.column.TsTzRangeTypeDescription
import io.github.clasicrando.kdbc.postgresql.column.UuidArrayTypeDescription
import io.github.clasicrando.kdbc.postgresql.column.UuidTypeDescription
import io.github.clasicrando.kdbc.postgresql.column.VarcharArrayTypeDescription
import io.github.clasicrando.kdbc.postgresql.column.VarcharTypeDescription
import io.github.clasicrando.kdbc.postgresql.column.XmlArrayTypeDescription
import io.github.clasicrando.kdbc.postgresql.type.PgBox
import io.github.clasicrando.kdbc.postgresql.type.PgCircle
import io.github.clasicrando.kdbc.postgresql.type.PgInet
import io.github.clasicrando.kdbc.postgresql.type.PgJson
import io.github.clasicrando.kdbc.postgresql.type.PgLine
import io.github.clasicrando.kdbc.postgresql.type.PgLineSegment
import io.github.clasicrando.kdbc.postgresql.type.PgMacAddress
import io.github.clasicrando.kdbc.postgresql.type.PgMoney
import io.github.clasicrando.kdbc.postgresql.type.PgPath
import io.github.clasicrando.kdbc.postgresql.type.PgPoint
import io.github.clasicrando.kdbc.postgresql.type.PgPolygon
import io.github.clasicrando.kdbc.postgresql.type.PgRange
import io.github.clasicrando.kdbc.postgresql.type.PgTimeTz
import kotlinx.datetime.DateTimePeriod
import kotlinx.datetime.Instant
import kotlinx.datetime.LocalDate
import kotlinx.datetime.LocalDateTime
import kotlinx.datetime.LocalTime
import kotlinx.datetime.TimeZone
import kotlinx.datetime.toInstant
import kotlinx.uuid.UUID
import java.math.BigDecimal
import kotlin.reflect.KType

class PgEncodeBuffer(
    private val metadata: List<PgColumnDescription>,
    private val typeCache: PgTypeCache,
) : AutoRelease {
    internal val innerBuffer: ByteWriteBuffer = ByteListWriteBuffer()
    var paramCount = 0
        private set
    private val innerTypes = mutableListOf<Int>()
    val types: List<Int> get() = innerTypes

    private fun encodeJson(value: PgJson) {
        val metadata = metadata[paramCount]
        when (metadata.pgType.oid) {
            PgType.JSON -> JsonTypeDescription.encode(value, innerBuffer)
            PgType.JSONB -> JsonbTypeDescription.encode(value, innerBuffer)
            else -> error(
                "Supplied parameter is PgJson but the parameter should be ${metadata.pgType}"
            )
        }
    }

    private fun encodeMacAddress(value: PgMacAddress) {
        val metadata = metadata[paramCount]
        when (metadata.pgType.oid) {
            PgType.MACADDR -> MacAddressTypeDescription.encode(value, innerBuffer)
            PgType.MACADDR8 -> MacAddress8TypeDescription.encode(value, innerBuffer)
            else -> error(
                "Supplied parameter is PgMacAddress but the parameter should be ${metadata.pgType}"
            )
        }
    }

    private fun encodeInet(value: PgInet) {
        val metadata = metadata[paramCount]
        when (metadata.pgType.oid) {
            PgType.INET -> InetTypeDescription.encode(value, innerBuffer)
            PgType.CIDR -> CidrTypeDescription.encode(value, innerBuffer)
            else -> error(
                "Supplied parameter is PgInet but the parameter should be ${metadata.pgType}"
            )
        }
    }

    @Suppress("UNCHECKED_CAST")
    private fun <T : Any> encodeRange(value: PgRange<T>, kType: KType) {
        when (kType) {
            Int8RangeTypeDescription.kType -> Int8RangeTypeDescription.encode(
                value = value as Int8Range,
                buffer = innerBuffer,
            )
            Int4RangeTypeDescription.kType -> Int4RangeTypeDescription.encode(
                value = value as Int4Range,
                buffer = innerBuffer,
            )
            TsRangeTypeDescription.kType -> TsRangeTypeDescription.encode(
                value = value as TsRange,
                buffer = innerBuffer,
            )
            TsTzRangeTypeDescription.kType -> TsTzRangeTypeDescription.encode(
                value = value as TsTzRange,
                buffer = innerBuffer,
            )
            DateRangeTypeDescription.kType -> DateRangeTypeDescription.encode(
                value = value as DateRange,
                buffer = innerBuffer,
            )
            NumRangeTypeDescription.kType -> NumRangeTypeDescription.encode(
                value = value as NumRange,
                buffer = innerBuffer,
            )
        }
    }

    private fun encodeStringList(value: List<String?>) {
        val metadata = metadata[paramCount]
        when (metadata.pgType) {
            PgType.VarcharArray -> VarcharArrayTypeDescription.encode(
                value = value,
                buffer = innerBuffer,
            )
            PgType.TextArray -> TextArrayTypeDescription.encode(
                value = value,
                buffer = innerBuffer,
            )
            PgType.BpcharArray -> BpcharArrayTypeDescription.encode(
                value = value,
                buffer = innerBuffer,
            )
            PgType.NameArray -> NameArrayTypeDescription.encode(
                value = value,
                buffer = innerBuffer,
            )
            PgType.XmlArray -> XmlArrayTypeDescription.encode(
                value = value,
                buffer = innerBuffer,
            )
            else -> error(
                "Supplied parameter type is List<String> but the expected parameter type is " +
                        "${metadata.pgType}"
            )
        }
    }

    private fun encodeJsonList(value: List<PgJson?>) {
        val metadata = metadata[paramCount]
        when (metadata.pgType.oid) {
            PgType.JSON_ARRAY -> JsonArrayTypeDescription.encode(
                value = value,
                buffer = innerBuffer,
            )
            PgType.JSONB_ARRAY -> JsonbArrayTypeDescription.encode(
                value = value,
                buffer = innerBuffer,
            )
            else -> error(
                "Supplied parameter type is List<PgJson> but the expected parameter type is " +
                        "${metadata.pgType}"
            )
        }
    }

    private fun encodeMacAddressList(value: List<PgMacAddress?>) {
        val metadata = metadata[paramCount]
        when (metadata.pgType.oid) {
            PgType.MACADDR_ARRAY -> MacAddressArrayTypeDescription.encode(
                value = value,
                buffer = innerBuffer,
            )
            PgType.MACADDR8_ARRAY -> MacAddress8ArrayTypeDescription.encode(
                value = value,
                buffer = innerBuffer,
            )
            else -> error(
                "Supplied parameter type is List<PgMacAddress> but the expected parameter type " +
                        "is ${metadata.pgType}"
            )
        }
    }

    private fun encodeInetList(value: List<PgInet?>) {
        val metadata = metadata[paramCount]
        when (metadata.pgType.oid) {
            PgType.INET_ARRAY -> InetArrayTypeDescription.encode(
                value = value,
                buffer = innerBuffer,
            )
            PgType.CIDR_ARRAY -> CidrArrayTypeDescription.encode(
                value = value,
                buffer = innerBuffer,
            )
            else -> error(
                "Supplied parameter is List<PgInet> but the expected parameter type is " +
                        "${metadata.pgType}"
            )
        }
    }

    @Suppress("UNCHECKED_CAST")
    private fun <T : Any> encodeList(value: List<T?>, kType: KType) {
        when (kType) {
            VarcharArrayTypeDescription.kType -> encodeStringList(value as List<String?>)
            ByteaArrayTypeDescription.kType -> ByteaArrayTypeDescription.encode(
                value = value as List<ByteArray?>,
                buffer = innerBuffer,
            )
            CharArrayTypeDescription.kType -> CharArrayTypeDescription.encode(
                value = value as List<Byte?>,
                buffer = innerBuffer,
            )
            SmallIntArrayTypeDescription.kType -> SmallIntArrayTypeDescription.encode(
                value = value as List<Short?>,
                buffer = innerBuffer,
            )
            IntArrayTypeDescription.kType -> IntArrayTypeDescription.encode(
                value = value as List<Int?>,
                buffer = innerBuffer,
            )
            BigIntArrayTypeDescription.kType -> BigIntArrayTypeDescription.encode(
                value = value as List<Long?>,
                buffer = innerBuffer,
            )
            RealArrayTypeDescription.kType -> RealArrayTypeDescription.encode(
                value = value as List<Float?>,
                buffer = innerBuffer,
            )
            DoublePrecisionArrayTypeDescription.kType -> DoublePrecisionArrayTypeDescription.encode(
                value = value as List<Double?>,
                buffer = innerBuffer,
            )
            NumericArrayTypeDescription.kType -> NumericArrayTypeDescription.encode(
                value = value as List<BigDecimal?>,
                buffer = innerBuffer,
            )
            TimeArrayTypeDescription.kType -> TimeArrayTypeDescription.encode(
                value = value as List<LocalTime?>,
                buffer = innerBuffer,
            )
            DateArrayTypeDescription.kType -> DateArrayTypeDescription.encode(
                value = value as List<LocalDate?>,
                buffer = innerBuffer,
            )
            TimestampArrayTypeDescription.kType -> TimestampArrayTypeDescription.encode(
                value = value as List<Instant?>,
                buffer = innerBuffer,
            )
            TimeTzArrayTypeDescription.kType -> TimeTzArrayTypeDescription.encode(
                value = value as List<PgTimeTz?>,
                buffer = innerBuffer,
            )
            TimestampTzArrayTypeDescription.kType -> TimestampTzArrayTypeDescription.encode(
                value = value as List<DateTime?>,
                buffer = innerBuffer,
            )
            IntervalArrayTypeDescription.kType -> IntervalArrayTypeDescription.encode(
                value = value as List<DateTimePeriod?>,
                buffer = innerBuffer,
            )
            UuidArrayTypeDescription.kType -> UuidArrayTypeDescription.encode(
                value = value as List<UUID?>,
                buffer = innerBuffer,
            )
            PointArrayTypeDescription.kType -> PointArrayTypeDescription.encode(
                value = value as List<PgPoint?>,
                buffer = innerBuffer,
            )
            LineArrayTypeDescription.kType -> LineArrayTypeDescription.encode(
                value = value as List<PgLine?>,
                buffer = innerBuffer,
            )
            LineSegmentArrayTypeDescription.kType -> LineSegmentArrayTypeDescription.encode(
                value = value as List<PgLineSegment?>,
                buffer = innerBuffer,
            )
            BoxArrayTypeDescription.kType -> BoxArrayTypeDescription.encode(
                value = value as List<PgBox?>,
                buffer = innerBuffer,
            )
            PathArrayTypeDescription.kType -> PathArrayTypeDescription.encode(
                value = value as List<PgPath?>,
                buffer = innerBuffer,
            )
            PolygonArrayTypeDescription.kType -> PolygonArrayTypeDescription.encode(
                value = value as List<PgPolygon?>,
                buffer = innerBuffer,
            )
            CircleArrayTypeDescription.kType -> CircleArrayTypeDescription.encode(
                value = value as List<PgCircle?>,
                buffer = innerBuffer,
            )
            JsonArrayTypeDescription.kType -> encodeJsonList(value as List<PgJson?>)
            MacAddressArrayTypeDescription.kType -> {
                encodeMacAddressList(value as List<PgMacAddress?>)
            }
            MoneyArrayTypeDescription.kType -> MoneyArrayTypeDescription.encode(
                value = value as List<PgMoney?>,
                buffer = innerBuffer,
            )
            InetArrayTypeDescription.kType -> encodeInetList(value as List<PgInet?>)
            BoolArrayTypeDescription.kType -> BoolArrayTypeDescription.encode(
                value = value as List<Boolean?>,
                buffer = innerBuffer,
            )
            Int8RangeArrayTypeDescription.kType -> Int8RangeArrayTypeDescription.encode(
                value = value as List<Int8Range?>,
                buffer = innerBuffer,
            )
            Int4RangeArrayTypeDescription.kType -> Int4RangeArrayTypeDescription.encode(
                value = value as List<Int4Range?>,
                buffer = innerBuffer,
            )
            TsRangeArrayTypeDescription.kType -> TsRangeArrayTypeDescription.encode(
                value = value as List<TsRange?>,
                buffer = innerBuffer,
            )
            TsTzRangeArrayTypeDescription.kType -> TsTzRangeArrayTypeDescription.encode(
                value = value as List<TsTzRange?>,
                buffer = innerBuffer,
            )
            DateRangeArrayTypeDescription.kType -> DateRangeArrayTypeDescription.encode(
                value = value as List<DateRange?>,
                buffer = innerBuffer,
            )
            NumRangeArrayTypeDescription.kType -> NumRangeArrayTypeDescription.encode(
                value = value as List<NumRange?>,
                buffer = innerBuffer,
            )
            else -> {
                typeCache.getTypeDescription<List<T?>>(kType)
                    ?.encode(value, innerBuffer)
                    ?: error(
                        "Could not get a type description from the custom type cache of the " +
                                "required type"
                    )
            }
        }
    }

    private fun <T : Any> encodeCustomType(value: T) {
        val metadata = metadata[paramCount]
        val typeDescription = typeCache.getTypeDescription<T>(metadata.pgType)
            ?: error(
                "Could not get a type description from the custom type cache of the required type"
            )
        typeDescription.encode(value, innerBuffer)
    }

    private fun <T : Any> encodeNonNullValue(value: T, kType: KType) {
        innerBuffer.writeLengthPrefixed {
            when (value) {
                is String -> VarcharTypeDescription.encode(value, innerBuffer)
                is Boolean -> BoolTypeDescription.encode(value, innerBuffer)
                is ByteArray -> ByteaTypeDescription.encode(value, innerBuffer)
                is Byte -> CharTypeDescription.encode(value, innerBuffer)
                is Short -> SmallIntTypeDescription.encode(value, innerBuffer)
                is Int -> IntTypeDescription.encode(value, innerBuffer)
                is Long -> BigIntTypeDescription.encode(value, innerBuffer)
                is Float -> RealTypeDescription.encode(value, innerBuffer)
                is Double -> DoublePrecisionTypeDescription.encode(value, innerBuffer)
                is BigDecimal -> NumericTypeDescription.encode(value, innerBuffer)
                is LocalTime -> TimeTypeDescription.encode(value, innerBuffer)
                is LocalDate -> DateTypeDescription.encode(value, innerBuffer)
                is LocalDateTime -> TimestampTypeDescription.encode(
                    value.toInstant(TimeZone.UTC),
                    innerBuffer
                )
                is Instant -> TimestampTypeDescription.encode(value, innerBuffer)
                is PgTimeTz -> TimeTzTypeDescription.encode(value, innerBuffer)
                is DateTime -> TimestampTzTypeDescription.encode(value, innerBuffer)
                is DateTimePeriod -> IntervalTypeDescription.encode(value, innerBuffer)
                is UUID -> UuidTypeDescription.encode(value, innerBuffer)
                is PgPoint -> PointTypeDescription.encode(value, innerBuffer)
                is PgLine -> LineTypeDescription.encode(value, innerBuffer)
                is PgLineSegment -> LineSegmentTypeDescription.encode(value, innerBuffer)
                is PgBox -> BoxTypeDescription.encode(value, innerBuffer)
                is PgPath -> PathTypeDescription.encode(value, innerBuffer)
                is PgPolygon -> PolygonTypeDescription.encode(value, innerBuffer)
                is PgCircle -> CircleTypeDescription.encode(value, innerBuffer)
                is PgJson -> encodeJson(value)
                is PgMacAddress -> encodeMacAddress(value)
                is PgMoney -> MoneyTypeDescription.encode(value, innerBuffer)
                is PgInet -> encodeInet(value)
                is PgRange<*> -> encodeRange(value, kType)
                is List<*> -> encodeList(value, kType)
                else -> encodeCustomType(value)
            }
        }
    }

    fun <T : Any> encodeValue(value: T?, kType: KType) {
        if (value == null) {
            paramCount++
            innerBuffer.writeInt(-1)
            return
        }
        encodeNonNullValue(value, kType)
        paramCount++
    }

    override fun release() {
        innerBuffer.release()
    }
}
