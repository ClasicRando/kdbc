package io.github.clasicrando.kdbc.postgresql.column

import com.ionspin.kotlin.bignum.decimal.BigDecimal
import io.github.clasicrando.kdbc.core.atomic.AtomicMutableMap
import io.github.clasicrando.kdbc.core.datetime.DateTime
import io.github.clasicrando.kdbc.core.query.QueryParameter
import io.github.clasicrando.kdbc.core.query.RowParser
import io.github.clasicrando.kdbc.core.query.bind
import io.github.clasicrando.kdbc.core.query.fetchAll
import io.github.clasicrando.kdbc.core.query.fetchScalar
import io.github.clasicrando.kdbc.core.result.DataRow
import io.github.clasicrando.kdbc.core.result.getAsNonNull
import io.github.clasicrando.kdbc.postgresql.connection.PgAsyncConnection
import io.github.clasicrando.kdbc.postgresql.connection.PgBlockingConnection
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
import io.github.oshai.kotlinlogging.KotlinLogging
import kotlinx.datetime.DateTimePeriod
import kotlinx.datetime.Instant
import kotlinx.datetime.LocalDate
import kotlinx.datetime.LocalDateTime
import kotlinx.datetime.LocalTime
import kotlinx.uuid.UUID
import kotlin.reflect.KClass
import kotlin.reflect.KType
import kotlin.reflect.full.createType

private val logger = KotlinLogging.logger {}

/**
 * Type cache for custom postgresql types as well as a lookup for the [PgType] of standard and
 * custom types (as a type hint when creating prepared statements). Instances of this class are
 * shared within connection pools so the contents and methods are thread-safe. This is accomplished
 * using [AtomicMutableMap]s for the lookup maps.
 */
@PublishedApi
internal class PgTypeCache {
    private val customTypeDescriptions: MutableMap<PgType, PgTypeDescription<*>> = AtomicMutableMap()
    private val typeHintLookup: MutableMap<KType, PgType> = AtomicMutableMap()

    /**
     * Return the custom type description for the provided [pgType]
     *
     * @throws IllegalStateException if the [pgType] cannot be found in the lookup table
     */
    @Suppress("UNCHECKED_CAST")
    fun <T : Any> getTypeDescription(pgType: PgType): PgTypeDescription<T>? {
        val typeDescription = customTypeDescriptions[pgType]
            ?: error("Could not find type description for pgType = $pgType")
        return typeDescription as? PgTypeDescription<T>
    }

    /**
     * Return the custom type description for the provided [kType]. Looks up the [kType] provided
     * to find a [PgType] then calls [getTypeDescription].
     *
     * @throws IllegalStateException if the [kType] cannot be found in the lookup table
     */
    fun <T : Any> getTypeDescription(kType: KType): PgTypeDescription<T>? {
        val pgType = typeHintLookup[kType]
            ?: error("Could not find type description for kType = $kType")
        return getTypeDescription(pgType)
    }

    /** Add a custom [typeDescription] to the lookup tables */
    private fun <T : Any> addTypeDescription(typeDescription: PgTypeDescription<T>) {
        customTypeDescriptions[typeDescription.pgType] = typeDescription
        typeHintLookup[typeDescription.kType] = typeDescription.pgType
    }

    /**
     * Add a new composite type description to the type cache. Uses the supplied [connection] to
     * query the database for metadata of the composite type (searching by [name]) for encoding and
     * decoding purposes. The generated [PgTypeDescription] is reflection based and has 2 checked
     * requirements:
     *
     * 1. [T] must be a data class
     * 2. The number of parameters supplied to [T] must match the number of attributes defined for
     * the composite type.
     *
     * The other requirements (such as the composite attribute types matching the data class) are
     * not checked at runtime so the class definer responsible for verifying them.
     */
    suspend fun <T : Any> addCompositeType(
        connection: PgAsyncConnection,
        name: String,
        cls: KClass<T>,
        compositeTypeDefinition: CompositeTypeDefinition<T>? = null,
    ) {
        val verifiedOid = checkCompositeDbTypeByName(connection, name)
            ?: error("Could not verify the composite type name '$name' in the database")

        val compositeColumnMapping = getCompositeAttributeData(connection, verifiedOid)

        val compositeTypeDescription = if (compositeTypeDefinition == null) {
            ReflectionCompositeTypeDescription(
                typeOid = verifiedOid,
                columnMapping = compositeColumnMapping,
                customTypeDescriptionCache = this,
                cls = cls,
            )
        } else {
            object : BaseCompositeTypeDescription<T>(
                typeOid = verifiedOid,
                columnMapping = compositeColumnMapping,
                customTypeDescriptionCache = this,
                kType = cls.createType(),
            ) {
                override fun extractValues(value: T): List<Pair<Any?, KType>> {
                    return compositeTypeDefinition.extractValues(value)
                }

                override fun fromRow(row: DataRow): T {
                    return compositeTypeDefinition.fromRow(row)
                }
            }
        }
        addTypeDescription(compositeTypeDescription)

        val arrayTypeOid = checkArrayDbTypeByOid(connection, verifiedOid)
            ?: error("Could not verify the array type for element oid = $verifiedOid")

        val compositeArrayTypeDescription = CompositeArrayTypeDescription(
            pgType = PgType.fromOid(arrayTypeOid),
            innerType = compositeTypeDescription,
        )
        addTypeDescription(compositeArrayTypeDescription)
    }

    /**
     * Add a new enum type definition to the type cache. Uses the supplied [connection] to get the
     * enums labels found in the database to compare against the supplied [enumValues]. This is the
     * only check that is required since the decoding and encoding is just reading and writing the
     * enum variants [Enum.name] value.
     */
    suspend fun <E : Enum<E>> addEnumType(
        connection: PgAsyncConnection,
        name: String,
        kType: KType,
        enumValues: Array<E>,
    ) {
        val verifiedOid = checkEnumDbTypeByName(connection, name)
            ?: error("Could not verify the composite type name '$name' in the database")

        val enumLabels = getEnumLabels(connection, verifiedOid)
        val enumTypeDescription = EnumTypeDescription(
            pgType = PgType.ByOid(oid = verifiedOid),
            kType = kType,
            values = enumValues,
        )
        val missingLabels = enumLabels.filter { !enumTypeDescription.entryLookup.contains(it) }
        check(missingLabels.isEmpty()) {
            "Cannot register an enum type because the declared enum values do not match the " +
                    "database's enum labels. Enum missing ${missingLabels.joinToString()}"
        }
        addTypeDescription(enumTypeDescription)

        val arrayTypeOid = checkArrayDbTypeByOid(connection, verifiedOid)
            ?: error("Could not verify the array type for element oid = $verifiedOid")

        val enumArrayTypeDescription = EnumArrayTypeDescription(
            pgType = PgType.fromOid(arrayTypeOid),
            innerType = enumTypeDescription,
        )
        addTypeDescription(enumArrayTypeDescription)
    }

    /**
     * Add a new composite type description to the type cache. Uses the supplied [connection] to
     * query the database for metadata of the composite type (searching by [name]) for encoding and
     * decoding purposes. The generated [PgTypeDescription] is reflection based and has 2 checked
     * requirements:
     *
     * 1. [T] must be a data class
     * 2. The number of parameters supplied to [T] must match the number of attributes defined for
     * the composite type.
     *
     * The other requirements (such as the composite attribute types matching the data class) are
     * not checked at runtime so the class definer responsible for verifying them.
     */
    fun <T : Any> addCompositeType(
        connection: PgBlockingConnection,
        name: String,
        cls: KClass<T>,
        compositeTypeDefinition: CompositeTypeDefinition<T>? = null,
    ) {
        val verifiedOid = checkCompositeDbTypeByName(connection, name)
            ?: error("Could not verify the composite type name '$name' in the database")

        val compositeColumnMapping = getCompositeAttributeData(connection, verifiedOid)

        val compositeTypeDescription = if (compositeTypeDefinition == null) {
            ReflectionCompositeTypeDescription(
                typeOid = verifiedOid,
                columnMapping = compositeColumnMapping,
                customTypeDescriptionCache = this,
                cls = cls,
            )
        } else {
            object : BaseCompositeTypeDescription<T>(
                typeOid = verifiedOid,
                columnMapping = compositeColumnMapping,
                customTypeDescriptionCache = this,
                kType = cls.createType(),
            ) {
                override fun extractValues(value: T): List<Pair<Any?, KType>> {
                    return compositeTypeDefinition.extractValues(value)
                }

                override fun fromRow(row: DataRow): T {
                    return compositeTypeDefinition.fromRow(row)
                }
            }
        }
        addTypeDescription(compositeTypeDescription)

        val arrayTypeOid = checkArrayDbTypeByOid(connection, verifiedOid)
            ?: error("Could not verify the array type for element oid = $verifiedOid")

        val compositeArrayTypeDescription = CompositeArrayTypeDescription(
            pgType = PgType.fromOid(arrayTypeOid),
            innerType = compositeTypeDescription,
        )
        addTypeDescription(compositeArrayTypeDescription)
    }

    /**
     * Add a new enum type definition to the type cache. Uses the supplied [connection] to get the
     * enums labels found in the database to compare against the supplied [enumValues]. This is the
     * only check that is required since the decoding and encoding is just reading and writing the
     * enum variants [Enum.name] value.
     */
    fun <E : Enum<E>> addEnumType(
        connection: PgBlockingConnection,
        name: String,
        kType: KType,
        enumValues: Array<E>,
    ) {
        val verifiedOid = checkEnumDbTypeByName(connection, name)
            ?: error("Could not verify the composite type name '$name' in the database")

        val enumLabels = getEnumLabels(connection, verifiedOid)
        val enumTypeDescription = EnumTypeDescription(
            pgType = PgType.ByOid(oid = verifiedOid),
            kType = kType,
            values = enumValues,
        )
        val missingLabels = enumLabels.filter { !enumTypeDescription.entryLookup.contains(it) }
        check(missingLabels.isEmpty()) {
            "Cannot register an enum type because the declared enum values do not match the " +
                    "database's enum labels. Enum missing ${missingLabels.joinToString()}"
        }
        addTypeDescription(enumTypeDescription)

        val arrayTypeOid = checkArrayDbTypeByOid(connection, verifiedOid)
            ?: error("Could not verify the array type for element oid = $verifiedOid")

        val enumArrayTypeDescription = EnumArrayTypeDescription(
            pgType = PgType.fromOid(arrayTypeOid),
            innerType = enumTypeDescription,
        )
        addTypeDescription(enumArrayTypeDescription)
    }

    /**
     * Get the [PgType] hint for the current [parameter]. This first checks the type of the value
     * to find easy matches to standard types, falling back to specialized methods for complex
     * types such as [PgRange] and [List], and as a last resort, checking the custom type lookup
     * by [KType] to find a type hint.
     */
    fun getTypeHint(parameter: QueryParameter): PgType {
        return when (parameter.value) {
            null -> PgType.Unspecified
            is String -> PgType.Text
            is Boolean -> PgType.Bool
            is ByteArray -> PgType.Bytea
            is Byte -> PgType.Char
            is Short -> PgType.Int2
            is Int -> PgType.Int4
            is Long -> PgType.Int8
            is Float -> PgType.Float4
            is Double -> PgType.Float8
            is BigDecimal -> PgType.Numeric
            is LocalTime -> PgType.Time
            is LocalDate -> PgType.Date
            is LocalDateTime -> PgType.Timestamp
            is Instant -> PgType.Timestamp
            is PgTimeTz -> PgType.Timetz
            is DateTime -> PgType.Timestamptz
            is DateTimePeriod -> PgType.Interval
            is UUID -> PgType.Uuid
            is PgPoint -> PgType.Point
            is PgLine -> PgType.Line
            is PgLineSegment -> PgType.LineSegment
            is PgBox -> PgType.Box
            is PgPath -> PgType.Path
            is PgPolygon -> PgType.Polygon
            is PgCircle -> PgType.Circle
            is PgJson -> PgType.Jsonb
            is PgMacAddress -> PgType.Macaddr8
            is PgMoney -> PgType.Money
            is PgInet -> PgType.Inet
            is PgRange<*> -> when (parameter.parameterType) {
                Int8RangeTypeDescription.kType -> PgType.Int8Range
                Int4RangeTypeDescription.kType -> PgType.Int4Range
                TsRangeTypeDescription.kType -> PgType.TsRange
                TsTzRangeTypeDescription.kType -> PgType.TstzRange
                DateRangeTypeDescription.kType -> PgType.DateRange
                NumRangeTypeDescription.kType -> PgType.NumRange
                else -> PgType.Unspecified
            }
            is List<*> -> when (parameter.parameterType) {
                VarcharArrayTypeDescription.kType -> PgType.TextArray
                ByteaArrayTypeDescription.kType -> PgType.ByteaArray
                CharArrayTypeDescription.kType -> PgType.CharArray
                SmallIntArrayTypeDescription.kType -> PgType.Int2Array
                IntArrayTypeDescription.kType -> PgType.Int4Array
                BigIntArrayTypeDescription.kType -> PgType.Int8Array
                RealArrayTypeDescription.kType -> PgType.Float4Array
                DoublePrecisionArrayTypeDescription.kType -> PgType.Float8Array
                NumericArrayTypeDescription.kType -> PgType.NumericArray
                TimeArrayTypeDescription.kType -> PgType.TimeArray
                DateArrayTypeDescription.kType -> PgType.DateArray
                TimestampArrayTypeDescription.kType -> PgType.TimestampArray
                TimeTzArrayTypeDescription.kType -> PgType.TimetzArray
                TimestampTzArrayTypeDescription.kType -> PgType.TimestamptzArray
                IntervalArrayTypeDescription.kType -> PgType.IntervalArray
                UuidArrayTypeDescription.kType -> PgType.UuidArray
                PointArrayTypeDescription.kType -> PgType.PointArray
                LineArrayTypeDescription.kType -> PgType.LineArray
                LineSegmentArrayTypeDescription.kType -> PgType.LineSegmentArray
                BoxArrayTypeDescription.kType -> PgType.BoxArray
                PathArrayTypeDescription.kType -> PgType.PathArray
                PolygonArrayTypeDescription.kType -> PgType.PolygonArray
                CircleArrayTypeDescription.kType -> PgType.CircleArray
                JsonArrayTypeDescription.kType -> PgType.JsonArray
                MacAddressArrayTypeDescription.kType -> PgType.MacaddrArray
                MoneyArrayTypeDescription.kType -> PgType.MoneyArray
                InetArrayTypeDescription.kType -> PgType.InetArray
                BoolArrayTypeDescription.kType -> PgType.BoolArray
                Int8RangeArrayTypeDescription.kType -> PgType.Int8RangeArray
                Int4RangeArrayTypeDescription.kType -> PgType.Int4RangeArray
                TsRangeArrayTypeDescription.kType -> PgType.TsRangeArray
                TsTzRangeArrayTypeDescription.kType -> PgType.TstzRangeArray
                DateRangeArrayTypeDescription.kType -> PgType.DateRangeArray
                NumRangeArrayTypeDescription.kType -> PgType.NumRangeArray
                else -> typeHintLookup[parameter.parameterType] ?: PgType.Unspecified
            }
            else -> typeHintLookup[parameter.parameterType] ?: PgType.Unspecified
        }
    }

    companion object {
        /**
         * Query to fetch the OID of an enum using the name and the optional schema. Default schema
         * is public.
         */
        private val pgEnumTypeByName =
            """
            select t.oid
            from pg_type t
            join pg_namespace n on t.typnamespace = n.oid
            where
                t.typname = $1
                and n.nspname = coalesce(nullif($2,''), 'public')
                and t.typcategory = 'E'
            """.trimIndent()

        /** Query to fetch the labels of an enum type given the OID of the type */
        private val pgEnumLabelsByOid =
            """
            select e.enumlabel
            from pg_enum e
            join pg_type t on e.enumtypid = t.oid
            where
                e.enumtypid = $1
                and t.typcategory = 'E'
            """.trimIndent()

        /**
         * Query to fetch the OID of a composite using the name and the optional schema. Default
         * schema is public.
         */
        private val pgCompositeTypeByName =
            """
            select t.oid
            from pg_type t
            join pg_namespace n on t.typnamespace = n.oid
            where
                t.typname = $1
                and n.nspname = coalesce(nullif($2,''), 'public')
                and t.typcategory = 'C'
            """.trimIndent()

        /**
         * Query to fetch the attribute related data of a composite given the OID of the composite
         * type
         */
        private val pgCompositeTypeDetailsByOid =
            """
            select a.attname, a.attrelid, a.attnum, a.atttypid, a.attlen, a.atttypmod
            from pg_type t
            join pg_attribute a on t.typrelid = a.attrelid
            where
                t.oid = $1
                and t.typcategory = 'C'
                and a.attnum > 0
            """.trimIndent()

        /** Query to fetch the OID of the array type with an inner type matching the OID supplied */
        private val pgArrayTypeByInnerOid =
            """
            select typarray
            from pg_type
            where oid = $1
            """.trimIndent()

        /**
         * Fetch and return the type OID for a composite with the [name]. Queries the database
         * using the [connection] provided to retrieve the database instance specific OID. Returns
         * null if the OID could not be found.
         *
         * @param name Name of the composite type. Can be schema qualified but defaults to public
         * if no schema is included
         */
        private suspend fun checkCompositeDbTypeByName(
            connection: PgAsyncConnection,
            name: String,
        ): Int? {
            var schema: String? = null
            var typeName = name
            val schemaQualifierIndex = name.indexOf('.')
            if (schemaQualifierIndex > -1) {
                schema = name.substring(0, schemaQualifierIndex)
                typeName = name.substring(schemaQualifierIndex + 1)
            }

            val oid = connection.createPreparedQuery(pgCompositeTypeByName)
                .bind(typeName)
                .bind(schema)
                .fetchScalar<Int>()
            if (oid == null) {
                logger.atWarn {
                    message = "Could not find composite type by name = '$name'"
                }
                return null
            }
            return oid
        }

        /**
         * Fetch and return the [PgColumnDescription]s for the composite type specified by [oid].
         * Queries the database using the [connection] to retrieve metadata about the composite
         * type's attributes.
         */
        private suspend fun getCompositeAttributeData(
            connection: PgAsyncConnection,
            oid: Int,
        ): List<PgColumnDescription> {
            return connection.createPreparedQuery(pgCompositeTypeDetailsByOid)
                .bind(oid)
                .fetchAll(CompositeAttributeDataRowParser)
        }

        /**
         * Fetch and return the type OID for an enum with the [name]. Queries the database using
         * the [connection] provided to retrieve the database instance specific OID. Returns null
         * if the OID could not be found.
         *
         * @param name Name of the enum type. Can be schema qualified but defaults to public if no
         * schema is included
         */
        private suspend fun checkEnumDbTypeByName(
            connection: PgAsyncConnection,
            name: String,
        ): Int? {
            var schema: String? = null
            var typeName = name
            val schemaQualifierIndex = name.indexOf('.')
            if (schemaQualifierIndex > -1) {
                schema = name.substring(0, schemaQualifierIndex)
                typeName = name.substring(schemaQualifierIndex + 1)
            }

            val oid = connection.createPreparedQuery(pgEnumTypeByName)
                .bind(typeName)
                .bind(schema)
                .fetchScalar<Int>()
            if (oid == null) {
                logger.atWarn {
                    message = "Could not find enum type for name = '$name'"
                }
                return null
            }
            return oid
        }

        /**
         * Fetch and return the labels of an enum type specified by the [oid]. Queries the database
         * using the [connection] provided to retrieve the labels.
         */
        private suspend fun getEnumLabels(
            connection: PgAsyncConnection,
            oid: Int,
        ): List<String> {
            return connection.createPreparedQuery(pgEnumLabelsByOid)
                .bind(oid)
                .fetchAll(EnumLabelRowParser)
        }

        /**
         * Fetch and return the array OID for a type whose inner [oid] is specified. Queries the
         * database using the [connection] provided to retrieve the database instance specific OID.
         * Returns null if the OID could not be found.
         */
        private suspend fun checkArrayDbTypeByOid(
            connection: PgAsyncConnection,
            oid: Int
        ): Int? {
            val arrayOid = connection.createPreparedQuery(pgArrayTypeByInnerOid)
                .bind(oid)
                .fetchScalar<Int>()

            if (arrayOid == null) {
                logger.atWarn {
                    message = "Could not find array type by oid = $oid"
                }
                return null
            }
            return arrayOid
        }

        /**
         * Fetch and return the type OID for a composite with the [name]. Queries the database
         * using the [connection] provided to retrieve the database instance specific OID. Returns
         * null if the OID could not be found.
         *
         * @param name Name of the composite type. Can be schema qualified but defaults to public
         * if no schema is included
         */
        private fun checkCompositeDbTypeByName(
            connection: PgBlockingConnection,
            name: String,
        ): Int? {
            var schema: String? = null
            var typeName = name
            val schemaQualifierIndex = name.indexOf('.')
            if (schemaQualifierIndex > -1) {
                schema = name.substring(0, schemaQualifierIndex)
                typeName = name.substring(schemaQualifierIndex + 1)
            }

            val oid = connection.createPreparedQuery(pgCompositeTypeByName)
                .bind(typeName)
                .bind(schema)
                .fetchScalar<Int>()
            if (oid == null) {
                logger.atWarn {
                    message = "Could not find composite type by name = '$name'"
                }
                return null
            }
            return oid
        }

        /**
         * Fetch and return the [PgColumnDescription]s for the composite type specified by [oid].
         * Queries the database using the [connection] to retrieve metadata about the composite
         * type's attributes.
         */
        private fun getCompositeAttributeData(
            connection: PgBlockingConnection,
            oid: Int,
        ): List<PgColumnDescription> {
            return connection.createPreparedQuery(pgCompositeTypeDetailsByOid)
                .bind(oid)
                .fetchAll(CompositeAttributeDataRowParser)
        }

        /**
         * Fetch and return the type OID for an enum with the [name]. Queries the database using
         * the [connection] provided to retrieve the database instance specific OID. Returns null
         * if the OID could not be found.
         *
         * @param name Name of the enum type. Can be schema qualified but defaults to public if no
         * schema is included
         */
        private fun checkEnumDbTypeByName(
            connection: PgBlockingConnection,
            name: String,
        ): Int? {
            var schema: String? = null
            var typeName = name
            val schemaQualifierIndex = name.indexOf('.')
            if (schemaQualifierIndex > -1) {
                schema = name.substring(0, schemaQualifierIndex)
                typeName = name.substring(schemaQualifierIndex + 1)
            }

            val oid = connection.createPreparedQuery(pgEnumTypeByName)
                .bind(typeName)
                .bind(schema)
                .fetchScalar<Int>()
            if (oid == null) {
                logger.atWarn {
                    message = "Could not find enum type for name = '$name'"
                }
                return null
            }
            return oid
        }

        /**
         * Fetch and return the labels of an enum type specified by the [oid]. Queries the database
         * using the [connection] provided to retrieve the labels.
         */
        private fun getEnumLabels(
            connection: PgBlockingConnection,
            oid: Int,
        ): List<String> {
            return connection.createPreparedQuery(pgEnumLabelsByOid)
                .bind(oid)
                .fetchAll(EnumLabelRowParser)
        }

        /**
         * Fetch and return the array OID for a type whose inner [oid] is specified. Queries the
         * database using the [connection] provided to retrieve the database instance specific OID.
         * Returns null if the OID could not be found.
         */
        private fun checkArrayDbTypeByOid(
            connection: PgBlockingConnection,
            oid: Int
        ): Int? {
            val arrayOid = connection.createPreparedQuery(pgArrayTypeByInnerOid)
                .bind(oid)
                .fetchScalar<Int>()

            if (arrayOid == null) {
                logger.atWarn {
                    message = "Could not find array type by oid = $oid"
                }
                return null
            }
            return arrayOid
        }

        /** [RowParser] for parsing the query result of composite type attributes */
        object CompositeAttributeDataRowParser : RowParser<PgColumnDescription> {
            override fun fromRow(row: DataRow): PgColumnDescription {
                return PgColumnDescription(
                    fieldName = row.getAsNonNull("attname"),
                    tableOid = row.getAsNonNull("attrelid"),
                    columnAttribute = row.getAsNonNull("attnum"),
                    pgType = PgType.fromOid(row.getAsNonNull("atttypid")),
                    dataTypeSize = row.getAsNonNull("attlen"),
                    typeModifier = row.getAsNonNull("atttypmod"),
                    formatCode = 0,
                )
            }
        }

        /** [RowParser] for parsing the query result of enum type labels */
        object EnumLabelRowParser : RowParser<String> {
            override fun fromRow(row: DataRow): String {
                return row.getAsNonNull("enumlabel")
            }
        }
    }
}
