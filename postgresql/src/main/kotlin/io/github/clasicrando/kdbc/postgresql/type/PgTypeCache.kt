package io.github.clasicrando.kdbc.postgresql.type

import io.github.clasicrando.kdbc.core.atomic.AtomicMutableMap
import io.github.clasicrando.kdbc.core.exceptions.KdbcException
import io.github.clasicrando.kdbc.core.query.QueryParameter
import io.github.clasicrando.kdbc.core.query.bind
import io.github.clasicrando.kdbc.core.query.fetchScalar
import io.github.clasicrando.kdbc.core.query.query
import io.github.clasicrando.kdbc.postgresql.connection.PgConnection
import io.github.clasicrando.kdbc.postgresql.exceptions.PgException
import io.github.oshai.kotlinlogging.KotlinLogging
import java.time.ZoneOffset
import kotlin.reflect.KType

private val logger = KotlinLogging.logger {}

/**
 * Type cache for custom postgresql types as well as a lookup for the [PgType] of standard and
 * custom types (as a type hint when creating prepared statements). Instances of this class are
 * shared within connection pools so the contents and methods are thread-safe. This is accomplished
 * using [AtomicMutableMap]s for the lookup maps.
 */
@PublishedApi
internal class PgTypeCache(zoneOffset: ZoneOffset) {
    private val typeDescriptions: MutableMap<KType, PgTypeDescription<*>> =
        AtomicMutableMap(getBaseTypes(zoneOffset))

    /**
     * Return the custom type description for the provided [kType]
     *
     * @throws KdbcException if the [kType] cannot be found in the lookup table
     */
    @Suppress("UNCHECKED_CAST")
    fun <T : Any> getTypeDescription(kType: KType): PgTypeDescription<T> {
        val typeDescription =
            typeDescriptions[kType] ?: throw PgException("No type description for $kType")
        return typeDescription as? PgTypeDescription<T>
            ?: throw PgException("Could not cast type description found")
    }

    /** Add a custom [typeDescription] to the lookup tables */
    private fun <T : Any> addTypeDescription(typeDescription: PgTypeDescription<T>) {
        typeDescriptions[typeDescription.kType] = typeDescription
    }

    /**
     * Add array type descriptions for the supplied type and array type. This includes `List<T>` and
     * `List<T?>`.
     */
    private fun <T : Any> addArrayTypeDescriptions(
        arrayType: PgType,
        typeDescription: PgTypeDescription<T>,
    ) {
        for (type in createArrayDescriptions(arrayType, typeDescription)) {
            addTypeDescription(type)
        }
    }

    /**
     * Add a new enum type definition to the type cache. Uses the supplied [connection] to get the
     * enums labels found in the database to compare against the supplied [enumValues]. This is the
     * only check that is required since the decoding and encoding is just reading and writing the
     * enum variants [Enum.name] value.
     */
    suspend fun <T : Any> addCustomType(
        connection: PgConnection,
        typeDescription: PgTypeDescription<T>,
    ) {
        addTypeDescription(typeDescription)
        val oid = typeDescription.dbType.oid
        val arrayTypeOid =
            checkArrayDbTypeByOid(connection, oid)
                ?: throw PgException("Could not verify the array type for element oid = $oid")
        addArrayTypeDescriptions(
            arrayType = PgType.fromOid(arrayTypeOid),
            typeDescription = typeDescription,
        )
    }

    /**
     * Get the [PgType] hint for the current [parameter]. This first checks the type of the value to
     * find easy matches to standard types, falling back to specialized methods for complex types
     * such as [PgRange] and [List], and as a last resort, checking the custom type lookup by
     * [KType] to find a type hint.
     */
    @Suppress("UNCHECKED_CAST")
    fun getTypeHint(parameter: QueryParameter): PgType {
        val parameterValue = parameter.value ?: return PgType.Unspecified
        val description = getTypeDescription<Any>(parameter.parameterType)
        return description.getActualType(parameterValue)
    }

    companion object {
        fun getBaseTypes(zoneOffset: ZoneOffset): Map<KType, PgTypeDescription<*>> {
            val offsetDateTimeDescription = OffsetDateTimeTypeDescription(zoneOffset)
            val instantDescription = InstantTypeDescription(zoneOffset)
            val timeZoneTzDescription = TsTzRangeTypeDescription(zoneOffset)
            return listOf(
                    BigDecimalTypeDescription,
                    *createArrayDescriptions(PgType.NumericArray, BigDecimalTypeDescription),
                    BoolTypeDescription,
                    *createArrayDescriptions(PgType.BoolArray, BoolTypeDescription),
                    ByteaTypeDescription,
                    *createArrayDescriptions(PgType.ByteaArray, ByteaTypeDescription),
                    CharTypeDescription,
                    *createArrayDescriptions(PgType.CharArray, CharTypeDescription),
                    LocalDateTypeDescription,
                    *createArrayDescriptions(PgType.DateArray, LocalDateTypeDescription),
                    LocalDateTimeTypeDescription,
                    *createArrayDescriptions(PgType.TimestampArray, LocalDateTimeTypeDescription),
                    offsetDateTimeDescription,
                    *createArrayDescriptions(PgType.TimestamptzArray, offsetDateTimeDescription),
                    instantDescription,
                    *createArrayDescriptions(PgType.TimestamptzArray, instantDescription),
                    PgIntervalTypeDescription,
                    *createArrayDescriptions(PgType.IntervalArray, PgIntervalTypeDescription),
                    DurationTypeDescription,
                    *createArrayDescriptions(PgType.IntervalArray, DurationTypeDescription),
                    PointTypeDescription,
                    *createArrayDescriptions(PgType.PointArray, PointTypeDescription),
                    LineTypeDescription,
                    *createArrayDescriptions(PgType.LineArray, LineTypeDescription),
                    LineSegmentTypeDescription,
                    *createArrayDescriptions(PgType.LineSegmentArray, LineSegmentTypeDescription),
                    BoxTypeDescription,
                    *createArrayDescriptions(PgType.BoxArray, BoxTypeDescription),
                    PathTypeDescription,
                    *createArrayDescriptions(PgType.PathArray, PathTypeDescription),
                    PolygonTypeDescription,
                    *createArrayDescriptions(PgType.PolygonArray, PolygonTypeDescription),
                    CircleTypeDescription,
                    *createArrayDescriptions(PgType.CircleArray, CircleTypeDescription),
                    JsonTypeDescription,
                    *createArrayDescriptions(PgType.JsonArray, JsonTypeDescription),
                    JsonBytesTypeDescription,
                    *createArrayDescriptions(PgType.JsonArray, JsonBytesTypeDescription),
                    JsonTextTypeDescription,
                    *createArrayDescriptions(PgType.JsonArray, JsonTextTypeDescription),
                    JsonPathTypeDescription,
                    *createArrayDescriptions(PgType.JsonpathArray, JsonPathTypeDescription),
                    MacAddressTypeDescription,
                    *createArrayDescriptions(PgType.MacaddrArray, MacAddressTypeDescription),
                    MoneyTypeDescription,
                    *createArrayDescriptions(PgType.MoneyArray, MoneyTypeDescription),
                    NetworkAddressTypeDescription,
                    *createArrayDescriptions(PgType.InetArray, NetworkAddressTypeDescription),
                    SmallIntTypeDescription,
                    *createArrayDescriptions(PgType.Int2Array, SmallIntTypeDescription),
                    IntTypeDescription,
                    *createArrayDescriptions(PgType.Int4Array, IntTypeDescription),
                    BigIntTypeDescription,
                    *createArrayDescriptions(PgType.Int8Array, BigIntTypeDescription),
                    RealTypeDescription,
                    *createArrayDescriptions(PgType.Float4Array, RealTypeDescription),
                    DoublePrecisionTypeDescription,
                    *createArrayDescriptions(PgType.Float8Array, DoublePrecisionTypeDescription),
                    Int8RangeTypeDescription,
                    *createArrayDescriptions(PgType.Int8RangeArray, Int8RangeTypeDescription),
                    Int4RangeTypeDescription,
                    *createArrayDescriptions(PgType.Int4RangeArray, Int4RangeTypeDescription),
                    TsRangeTypeDescription,
                    *createArrayDescriptions(PgType.TsRangeArray, TsRangeTypeDescription),
                    timeZoneTzDescription,
                    *createArrayDescriptions(PgType.TstzRangeArray, timeZoneTzDescription),
                    DateRangeTypeDescription,
                    *createArrayDescriptions(PgType.DateRangeArray, DateRangeTypeDescription),
                    NumRangeTypeDescription,
                    *createArrayDescriptions(PgType.NumRangeArray, NumRangeTypeDescription),
                    VarcharTypeDescription,
                    object :
                        ArrayTypeDescription<String>(
                            pgType = PgType.Varchar,
                            innerType = VarcharTypeDescription,
                            innerNullable = true,
                        ) {
                        override fun isCompatible(dbType: PgType): Boolean =
                            dbType == PgType.TextArray ||
                                dbType == PgType.VarcharArray ||
                                dbType == PgType.XmlArray ||
                                dbType == PgType.NameArray ||
                                dbType == PgType.BpcharArray
                    },
                    object :
                        ArrayTypeDescription<String>(
                            pgType = PgType.Varchar,
                            innerType = VarcharTypeDescription,
                            innerNullable = false,
                        ) {
                        override fun isCompatible(dbType: PgType): Boolean =
                            dbType == PgType.TextArray ||
                                dbType == PgType.VarcharArray ||
                                dbType == PgType.XmlArray ||
                                dbType == PgType.NameArray ||
                                dbType == PgType.BpcharArray
                    },
                    LocalTimeTypeDescription,
                    *createArrayDescriptions(PgType.TimeArray, LocalTimeTypeDescription),
                    OffsetTimeTypeDescription,
                    *createArrayDescriptions(PgType.TimetzArray, OffsetTimeTypeDescription),
                    UuidTypeDescription,
                    *createArrayDescriptions(PgType.UuidArray, UuidTypeDescription),
                    JUuidTypeDescription,
                    *createArrayDescriptions(PgType.UuidArray, JUuidTypeDescription),
                )
                .associateBy { it.kType }
        }

        /** Query to fetch the OID of the array type with an inner type matching the OID supplied */
        private val pgArrayTypeByInnerOid =
            """
            select typarray
            from pg_type
            where oid = $1
            """
                .trimIndent()

        /**
         * Fetch and return the array OID for a type whose inner [oid] is specified. Queries the
         * database using the [connection] provided to retrieve the database instance specific OID.
         * Returns null if the OID could not be found.
         */
        private suspend fun checkArrayDbTypeByOid(connection: PgConnection, oid: Int): Int? {
            val arrayOid = query(pgArrayTypeByInnerOid).bind(oid).fetchScalar<Int>(connection)

            if (arrayOid == null) {
                logger.atWarn { message = "Could not find array type by oid = $oid" }
                return null
            }
            return arrayOid
        }
    }
}
