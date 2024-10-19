package io.github.clasicrando.kdbc.postgresql.type

import io.github.clasicrando.kdbc.core.atomic.AtomicMutableMap
import io.github.clasicrando.kdbc.core.exceptions.KdbcException
import io.github.clasicrando.kdbc.core.query.QueryParameter
import io.github.clasicrando.kdbc.core.query.RowParser
import io.github.clasicrando.kdbc.core.query.bind
import io.github.clasicrando.kdbc.core.query.fetchAll
import io.github.clasicrando.kdbc.core.query.fetchScalar
import io.github.clasicrando.kdbc.core.result.DataRow
import io.github.clasicrando.kdbc.core.result.getAsNonNull
import io.github.clasicrando.kdbc.postgresql.column.PgColumnDescription
import io.github.clasicrando.kdbc.postgresql.connection.PgConnection
import io.github.oshai.kotlinlogging.KotlinLogging
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
    private val typeDescriptions: MutableMap<KType, PgTypeDescription<*>> = AtomicMutableMap(baseTypes)

    /**
     * Return the custom type description for the provided [kType]
     *
     * @throws KdbcException if the [kType] cannot be found in the lookup table
     */
    @Suppress("UNCHECKED_CAST")
    fun <T : Any> getTypeDescription(kType: KType): PgTypeDescription<T>? {
        val typeDescription = typeDescriptions[kType]
            ?: throw KdbcException("No type description for $kType")
        return typeDescription as? PgTypeDescription<T>
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
        connection: PgConnection,
        name: String,
        cls: KClass<T>,
        compositeTypeDefinition: CompositeTypeDefinition<T>? = null,
    ) {
        val verifiedOid = checkCompositeDbTypeByName(connection, name)
            ?: error("Could not verify the composite type name '$name' in the database")

        val compositeColumnMapping = getCompositeAttributeData(connection, verifiedOid)

        val typeDef = compositeTypeDefinition ?: ReflectionCompositeTypeDescription(cls)
        val compositeTypeDescription = BaseCompositeTypeDescription(
            compositeTypeDefinition = typeDef,
            typeOid = verifiedOid,
            attributeMapping = compositeColumnMapping,
            typeCache = this,
            kType = cls.createType(),
        )
        addTypeDescription(compositeTypeDescription)

        val arrayTypeOid = checkArrayDbTypeByOid(connection, verifiedOid)
            ?: error("Could not verify the array type for element oid = $verifiedOid")
        addArrayTypeDescriptions(
            arrayType = PgType.fromOid(arrayTypeOid),
            typeDescription = compositeTypeDescription,
        )
    }

    /**
     * Add a new enum type definition to the type cache. Uses the supplied [connection] to get the
     * enums labels found in the database to compare against the supplied [enumValues]. This is the
     * only check that is required since the decoding and encoding is just reading and writing the
     * enum variants [Enum.name] value.
     */
    suspend fun <E : Enum<E>> addEnumType(
        connection: PgConnection,
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
        addArrayTypeDescriptions(
            arrayType = PgType.fromOid(arrayTypeOid),
            typeDescription = enumTypeDescription,
        )
    }

    /**
     * Add a new enum type definition to the type cache. Uses the supplied [connection] to get the
     * enums labels found in the database to compare against the supplied [enumValues]. This is the
     * only check that is required since the decoding and encoding is just reading and writing the
     * enum variants [Enum.name] value.
     */
    suspend fun <T : Any> addCustomType(
        connection: PgConnection,
        typeDescription: PgTypeDescription<T>
    ) {
        addTypeDescription(typeDescription)
        val oid = typeDescription.dbType.oid
        val arrayTypeOid = checkArrayDbTypeByOid(connection, oid)
            ?: error("Could not verify the array type for element oid = $oid")
        addArrayTypeDescriptions(
            arrayType = PgType.fromOid(arrayTypeOid),
            typeDescription = typeDescription,
        )
    }

    /**
     * Get the [PgType] hint for the current [parameter]. This first checks the type of the value
     * to find easy matches to standard types, falling back to specialized methods for complex
     * types such as [PgRange] and [List], and as a last resort, checking the custom type lookup
     * by [KType] to find a type hint.
     */
    @Suppress("UNCHECKED_CAST")
    fun getTypeHint(parameter: QueryParameter): PgType {
        val parameterValue = parameter.value ?: return PgType.Unspecified
        val description = typeDescriptions[parameter.parameterType] as? PgTypeDescription<Any>
            ?: return PgType.Unspecified
        return description.getActualType(parameterValue)
    }

    companion object {
        val baseTypes: Map<KType, PgTypeDescription<*>> = listOf(
            BigDecimalTypeDescription,
            *createArrayDescriptions(PgType.NumericArray, BigDecimalTypeDescription),
            JBigDecimalTypeDescription,
            *createArrayDescriptions(PgType.NumericArray, JBigDecimalTypeDescription),
            BoolTypeDescription,
            *createArrayDescriptions(PgType.BoolArray, BoolTypeDescription),
            ByteaTypeDescription,
            *createArrayDescriptions(PgType.ByteaArray, ByteaTypeDescription),
            CharTypeDescription,
            *createArrayDescriptions(PgType.CharArray, CharTypeDescription),
            LocalDateTypeDescription,
            *createArrayDescriptions(PgType.DateArray, LocalDateTypeDescription),
            JLocalDateTypeDescription,
            *createArrayDescriptions(PgType.DateArray, JLocalDateTypeDescription),
            InstantTypeDescription,
            *createArrayDescriptions(PgType.TimestampArray, InstantTypeDescription),
            LocalDateTimeTypeDescription,
            *createArrayDescriptions(PgType.TimestampArray, LocalDateTimeTypeDescription),
            DateTimeTypeDescription,
            *createArrayDescriptions(PgType.TimestamptzArray, DateTimeTypeDescription),
            OffsetDateTimeTypeDescription,
            *createArrayDescriptions(PgType.TimestamptzArray, OffsetDateTimeTypeDescription),
            DateTimePeriodTypeDescription,
            *createArrayDescriptions(PgType.IntervalArray, DateTimePeriodTypeDescription),
            PgIntervalTypeDescription,
            *createArrayDescriptions(PgType.IntervalArray, PgIntervalTypeDescription),
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
            JTsRangeTypeDescription,
            *createArrayDescriptions(PgType.TsRangeArray, JTsRangeTypeDescription),
            TsTzRangeTypeDescription,
            *createArrayDescriptions(PgType.TstzRangeArray, TsTzRangeTypeDescription),
            JTsTzRangeTypeDescription,
            *createArrayDescriptions(PgType.TstzRangeArray, JTsTzRangeTypeDescription),
            DateRangeTypeDescription,
            *createArrayDescriptions(PgType.DateRangeArray, DateRangeTypeDescription),
            JDateRangeTypeDescription,
            *createArrayDescriptions(PgType.DateRangeArray, JDateRangeTypeDescription),
            NumRangeTypeDescription,
            *createArrayDescriptions(PgType.NumRangeArray, NumRangeTypeDescription),
            VarcharTypeDescription,
            object : ArrayTypeDescription<String>(
                pgType = PgType.Varchar,
                innerType = VarcharTypeDescription,
                innerNullable = true
            ) {
                override fun isCompatible(dbType: PgType): Boolean {
                    return dbType == PgType.TextArray
                            || dbType == PgType.VarcharArray
                            || dbType == PgType.XmlArray
                            || dbType == PgType.NameArray
                            || dbType == PgType.BpcharArray
                }
            },
            object : ArrayTypeDescription<String>(
                pgType = PgType.Varchar,
                innerType = VarcharTypeDescription,
                innerNullable = false
            ) {
                override fun isCompatible(dbType: PgType): Boolean {
                    return dbType == PgType.TextArray
                            || dbType == PgType.VarcharArray
                            || dbType == PgType.XmlArray
                            || dbType == PgType.NameArray
                            || dbType == PgType.BpcharArray
                }
            },
            LocalTimeTypeDescription,
            *createArrayDescriptions(PgType.TimeArray, LocalTimeTypeDescription),
            JLocalTimeTypeDescription,
            *createArrayDescriptions(PgType.TimeArray, JLocalTimeTypeDescription),
            PgTimeTzTypeDescription,
            *createArrayDescriptions(PgType.TimetzArray, PgTimeTzTypeDescription),
            OffsetTimeTypeDescription,
            *createArrayDescriptions(PgType.TimetzArray, OffsetTimeTypeDescription),
            UuidTypeDescription,
            *createArrayDescriptions(PgType.UuidArray, UuidTypeDescription),
            JUuidTypeDescription,
            *createArrayDescriptions(PgType.UuidArray, JUuidTypeDescription),
        ).associateBy { it.kType }
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
            connection: PgConnection,
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
            connection: PgConnection,
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
            connection: PgConnection,
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
            connection: PgConnection,
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
            connection: PgConnection,
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
