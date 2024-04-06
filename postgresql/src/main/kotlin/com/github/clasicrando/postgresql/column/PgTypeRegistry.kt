package com.github.clasicrando.postgresql.column

import com.github.clasicrando.common.atomic.AtomicMutableMap
import com.github.clasicrando.common.buffer.ByteWriteBuffer
import com.github.clasicrando.common.column.ColumnDecodeError
import com.github.clasicrando.common.use
import com.github.clasicrando.postgresql.connection.PgBlockingConnection
import com.github.clasicrando.postgresql.connection.PgConnectOptions
import com.github.clasicrando.postgresql.connection.PgConnection
import io.github.oshai.kotlinlogging.KotlinLogging
import kotlinx.datetime.DateTimePeriod
import kotlin.reflect.KType
import kotlin.reflect.KTypeProjection
import kotlin.reflect.full.createType
import kotlin.reflect.full.isSubtypeOf
import kotlin.reflect.typeOf

private val logger = KotlinLogging.logger {}

/**
 * Cache of [PgTypeEncoder] and [PgTypeDecoder] instances for lookup when trying to read query
 * results or send parameters with queries. A single instance of this class is shared between
 * connections using the [PgConnectOptions] since they are backed by the same connection pool.
 *
 * Operating with this type is done through 3 channels:
 *
 * 1. Encoding a value into an argument buffer using [encode]
 * 2. Decoding a [PgValue] into the expected type using [decode]
 * 3. Adding custom types through [registerCompositeType] or [registerEnumType]
 */
@PublishedApi
internal class PgTypeRegistry {
    /**
     * Initial [PgTypeEncoder] map storing encoders by the [KType] they can decode. Since encoders
     * might be attached to multiple subtypes, the [defaultEncoders] is unpacked for each encode
     * type.
     */
    private val encoders: MutableMap<KType, PgTypeEncoder<*>> = AtomicMutableMap(
        buildMap {
            for (encoder in defaultEncoders) {
                for (type in encoder.encodeTypes) {
                    this[type] = encoder
                }
            }
        }
    )

    /**
     * Initial [PgTypeDecoder] map storing decoders by OID as [Int]. OIDs are unique so no need to
     * unpack the [defaultDecoders].
     */
    private val decoders: MutableMap<Int, PgTypeDecoder<*>> = AtomicMutableMap(defaultDecoders)
    private var hasPostGisTypes = false

    /**
     * Lookup the decoder of this [PgValue] and call [PgTypeDecoder.decode] to return a new
     * instance with the contents of [value] used to instantiate the type. This always decodes to
     * the Kotlin equivalent type indicated by the [PgType.oid]. Therefore, casting (safe or
     * unsafe) is required to get to the actual value.
     *
     * @throws ColumnDecodeError if the decode fails as per [PgTypeDecoder.decode]
     * @throws IllegalStateException if the oid of the [PgValue] cannot be found in the [decoders]
     * cache
     */
    fun decode(value: PgValue): Any {
        val oid = value.typeData.dataType
        return decoders[oid]
            ?.decode(value)
            ?: error("Could not find decoder when looking up oid = $oid")
    }

    /**
     * Lookup the encoder of this [value] and call [PgTypeEncoder.encode] to get the binary
     * representation of the [value] into the [buffer]. This has special cases for known types that
     * cannot be looked up properly by the [KType] of the [value] such as [List] (for array types)
     * and [DateTimePeriod] since 1 of the sealed class variants is not accessible outside its
     * package.
     *
     * @throws IllegalStateException if the encoder cannot be found in the [encoders] cache
     */
    fun encode(value: Any, buffer: ByteWriteBuffer) {
        return when (value) {
            is DateTimePeriod -> dateTimePeriodTypeEncoder.encode(value, buffer)
            is List<*> -> encodeList(value, buffer)
            else -> encodeInternal(value, buffer)
        }
    }

    /**
     * Special case for encoding [List] values into the [buffer] specified. The order of priority
     * is:
     *
     * - if the list is empty or all items are null, use [emptyOrNullOnlyArrayEncoder]
     * - if the first element is [DateTimePeriod] then use [dateTimePeriodArrayTypeEncoder]
     * - otherwise, attempt to create a [KType] using the first element's [KTypeProjection] and
     * look up the encoder of the [List] type in [encoders].
     *
     * @throws IllegalStateException if the encoder cannot be found in the [encoders] cache
     */
    @Suppress("UNCHECKED_CAST")
    private fun <T> encodeList(value: List<T>, buffer: ByteWriteBuffer) {
        if (value.isEmpty() || value.firstNotNullOfOrNull { it } == null) {
            emptyOrNullOnlyArrayEncoder.encode(value, buffer)
            return
        }
        val elementType = value[0]!!::class.createType(nullable = true)
        if (value[0] is DateTimePeriod) {
            dateTimePeriodArrayTypeEncoder.encode(value as List<DateTimePeriod>, buffer)
            return
        }
        val typeParameter = KTypeProjection.invariant(elementType)
        val type = List::class.createType(arguments = listOf(typeParameter))
        val encoder = encoders[type]
            ?: error("Could not find encoder when looking up type = $type")
        (encoder as PgTypeEncoder<List<T>>).encode(value, buffer)
    }

    /**
     * Create a [KType] instance for [T] and use that to look up the encoder in [encoders].
     *
     * @throws IllegalStateException if the encoder cannot be found in the [encoders] cache
     */
    @Suppress("UNCHECKED_CAST")
    private fun <T : Any> encodeInternal(value: T, buffer: ByteWriteBuffer) {
        val type = value::class.createType()
        val encoder = encoders[type]
            ?: error("Could not find encoder when looking up type = $type")
        (encoder as PgTypeEncoder<T>).encode(value, buffer)
    }

    /**
     * Look up the [PgType] given the [KType] provided. Uses [type] to search the [encoders] cache
     * for the referenced [PgTypeEncoder.pgType]. If that is null, check for the special case of
     * [DateTimePeriod] since that [PgType] is known and the [KType] might not be in the cache.
     *
     * @throws IllegalStateException if the [PgType] cannot be found
     */
    internal fun kindOfInternal(type: KType): PgType {
        val pgType = encoders[type]?.pgType
        if (pgType != null) {
            return pgType
        }
        return when {
            type.isSubtypeOf(dateTimePeriodType) -> dateTimePeriodTypeEncoder.pgType
            else -> error("Could not find type OID looking up type = $type")
        }
    }

    /**
     * Find the [PgType] of the provided [List].
     *
     * Usually this uses the first element in the [List] to create a [KTypeProjection] which is
     * used to create a new [KType] for [List] with the generic type parameter specified. This
     * allows for looking up the appropriate [List] [PgType]. If the [List] is empty or all items
     * are null [PgType.Unspecified] is returned. There is also a special case for lists with
     * [DateTimePeriod] as the element type since one of the sealed variants is not accessible
     * outside its package.
     */
    private fun <T> kindOfList(value: List<T>): PgType {
        if (value.isEmpty() || value.firstNotNullOfOrNull { it } == null) {
            return PgType.Unspecified
        }
        val elementType = value[0]!!::class.createType(nullable = true)
        if (elementType.isSubtypeOf(dateTimePeriodType)) {
            return dateTimePeriodArrayTypeEncoder.pgType
        }
        val typeParameter = KTypeProjection.invariant(elementType)
        val type = List::class.createType(arguments = listOf(typeParameter))
        return kindOfInternal(type)
    }

    /**
     * Find the [PgType] for the [value].
     *
     * This is handled in 4 cases:
     *
     * 1. when [value] is null, return [PgType.Unspecified]
     * 2. When [value] is a [List], call [kindOfList]
     * 3. Otherwise, call [kindOfInternal] with a [KType] created from [value]
     * 4. If an exception is thrown during one of the previous cases, return [PgType.Unspecified]
     */
    fun kindOf(value: Any?): PgType {
        return try {
            when (value) {
                null -> PgType.Unspecified
                is List<*> -> kindOfList(value)
                else -> kindOfInternal(value::class.createType())
            }
        } catch (ex: Throwable) {
            PgType.Unspecified
        }
    }

    /** Add geometry types into the type caches */
    internal fun includePostGisTypes() {
        if (hasPostGisTypes) {
            return
        }
        for (encoder in postGisEncoders) {
            for (type in encoder.encodeTypes) {
                encoders[type] = encoder
            }
        }
        for ((oid, decoder) in postGisDecoders) {
            decoders[oid] = decoder
        }
        hasPostGisTypes = true
    }

    /**
     * Check the current [encoders] cache for each [KType] and the [decoders] for the [oid] to see
     * if any entries already exist. If they exist, log a warning message.
     */
    private fun checkAgainstExistingType(types: List<KType>, oid: Int) {
        if (decoders.containsKey(oid)) {
            logger.atWarn {
                message = "Replacing type definition for oid = {oid}"
                payload = mapOf("oid" to oid)
            }
        }
        for (type in types) {
            if (encoders.containsKey(type)) {
                logger.atWarn {
                    message = "Replacing type definition for type = {type}"
                    payload = mapOf("type" to type)
                }
            }
        }
    }

    /**
     * Add the [encoder] to the [encoders] cache for each [PgTypeEncoder.encodeTypes] and add the
     * [decoder] for the [oid].
     */
    private fun <E : Any, D : Any> addTypeToCaches(
        oid: Int,
        encoder: PgTypeEncoder<E>,
        decoder: PgTypeDecoder<D>,
    ) {
        for (type in encoder.encodeTypes) {
            encoders[type] = encoder
        }
        decoders[oid] = decoder
    }

    /**
     * Register a new enum type with all the required components. Checks the database for the oid
     * of the enum type using the [connection] provided. Adds the type, and it's related array type
     * as well (looking up the oid in a similar fashion).
     */
    suspend fun <E : Enum<E>, D : Enum<D>> registerEnumType(
        connection: PgConnection,
        encoder: PgTypeEncoder<E>,
        decoder: PgTypeDecoder<D>,
        type: String,
        arrayType: KType,
    ) {
        val verifiedOid = checkEnumDbTypeByName(type, connection)
            ?: error("Could not verify the enum type name '$type' in the database")
        logger.atTrace {
            message = "Adding column decoder for enum type {name} ({oid})"
            payload = mapOf("name" to type, "oid" to verifiedOid)
        }
        checkAgainstExistingType(encoder.encodeTypes, verifiedOid)
        encoder.pgType = PgType.ByName(type, verifiedOid)
        addTypeToCaches(verifiedOid, encoder, decoder)

        val arrayOid = checkArrayDbTypeByOid(verifiedOid, connection)
            ?: error("Could not verify the array type for element oid = $verifiedOid")
        logger.atTrace {
            message = "Adding array column decoder for enum type {name} ({oid})"
            payload = mapOf("name" to type, "oid" to verifiedOid)
        }
        val arrayTypeEncoder = arrayTypeEncoder(
            encoder = encoder,
            pgType = PgType.ByName("_$type", arrayOid),
            arrayType = arrayType,
        )
        checkAgainstExistingType(arrayTypeEncoder.encodeTypes, arrayOid)
        addTypeToCaches(
            oid = arrayOid,
            encoder = arrayTypeEncoder,
            decoder = arrayTypeDecoder(decoder, arrayType),
        )
    }

    /**
     * Register a new composite type with all the required components. Checks the database for the
     * oid of the composite type using the [connection] provided. Adds the type, and it's related
     * array type as well (looking up the oid in a similar fashion).
     */
    suspend fun <E : Any, D : Any> registerCompositeType(
        connection: PgConnection,
        encoder: PgTypeEncoder<E>,
        decoder: PgTypeDecoder<D>,
        type: String,
        arrayType: KType,
    ) {
        val verifiedOid = checkCompositeDbTypeByName(type, connection)
            ?: error("Could not verify the composite type name '$type' in the database")
        logger.atTrace {
            message = "Adding column decoder for composite type {name} ({oid})"
            payload = mapOf("name" to type, "oid" to verifiedOid)
        }
        encoder.pgType = PgType.ByName(type, verifiedOid)
        checkAgainstExistingType(encoder.encodeTypes, verifiedOid)
        addTypeToCaches(verifiedOid, encoder, decoder)

        val arrayOid = checkArrayDbTypeByOid(verifiedOid, connection)
            ?: error("Could not verify the array type for element oid = $verifiedOid")
        logger.atTrace {
            message = "Adding array column decoder for composite type {name} ({oid})"
            payload = mapOf("name" to type, "oid" to verifiedOid)
        }
        val arrayTypeEncoder = arrayTypeEncoder(
            encoder = encoder,
            pgType = PgType.ByName("_$type", arrayOid),
            arrayType = arrayType,
        )
        checkAgainstExistingType(arrayTypeEncoder.encodeTypes, arrayOid)
        addTypeToCaches(
            oid = arrayOid,
            encoder = arrayTypeEncoder,
            decoder = arrayTypeDecoder(decoder, arrayType),
        )
    }

    /**
     * Register a new enum type with all the required components. Checks the database for the oid
     * of the enum type using the [connection] provided. Adds the type, and it's related array type
     * as well (looking up the oid in a similar fashion).
     */
    fun <E : Enum<E>, D : Enum<D>> registerEnumType(
        connection: PgBlockingConnection,
        encoder: PgTypeEncoder<E>,
        decoder: PgTypeDecoder<D>,
        type: String,
        arrayType: KType,
    ) {
        val verifiedOid = checkEnumDbTypeByName(type, connection)
            ?: error("Could not verify the enum type name '$type' in the database")
        logger.atTrace {
            message = "Adding column decoder for enum type {name} ({oid})"
            payload = mapOf("name" to type, "oid" to verifiedOid)
        }
        checkAgainstExistingType(encoder.encodeTypes, verifiedOid)
        encoder.pgType = PgType.ByName(type, verifiedOid)
        addTypeToCaches(verifiedOid, encoder, decoder)

        val arrayOid = checkArrayDbTypeByOid(verifiedOid, connection)
            ?: error("Could not verify the array type for element oid = $verifiedOid")
        logger.atTrace {
            message = "Adding array column decoder for enum type {name} ({oid})"
            payload = mapOf("name" to type, "oid" to verifiedOid)
        }
        val arrayTypeEncoder = arrayTypeEncoder(
            encoder = encoder,
            pgType = PgType.ByName("_$type", arrayOid),
            arrayType = arrayType,
        )
        checkAgainstExistingType(arrayTypeEncoder.encodeTypes, arrayOid)
        addTypeToCaches(
            oid = arrayOid,
            encoder = arrayTypeEncoder,
            decoder = arrayTypeDecoder(decoder, arrayType),
        )
    }

    /**
     * Register a new composite type with all the required components. Checks the database for the
     * oid of the composite type using the [connection] provided. Adds the type, and it's related
     * array type as well (looking up the oid in a similar fashion).
     */
    fun <E : Any, D : Any> registerCompositeType(
        connection: PgBlockingConnection,
        encoder: PgTypeEncoder<E>,
        decoder: PgTypeDecoder<D>,
        type: String,
        arrayType: KType,
    ) {
        val verifiedOid = checkCompositeDbTypeByName(type, connection)
            ?: error("Could not verify the composite type name '$type' in the database")
        logger.atTrace {
            message = "Adding column decoder for composite type {name} ({oid})"
            payload = mapOf("name" to type, "oid" to verifiedOid)
        }
        encoder.pgType = PgType.ByName(type, verifiedOid)
        checkAgainstExistingType(encoder.encodeTypes, verifiedOid)
        addTypeToCaches(verifiedOid, encoder, decoder)

        val arrayOid = checkArrayDbTypeByOid(verifiedOid, connection)
            ?: error("Could not verify the array type for element oid = $verifiedOid")
        logger.atTrace {
            message = "Adding array column decoder for composite type {name} ({oid})"
            payload = mapOf("name" to type, "oid" to verifiedOid)
        }
        val arrayTypeEncoder = arrayTypeEncoder(
            encoder = encoder,
            pgType = PgType.ByName("_$type", arrayOid),
            arrayType = arrayType,
        )
        checkAgainstExistingType(arrayTypeEncoder.encodeTypes, arrayOid)
        addTypeToCaches(
            oid = arrayOid,
            encoder = arrayTypeEncoder,
            decoder = arrayTypeDecoder(decoder, arrayType),
        )
    }

    companion object {
        private val dateTimePeriodType = typeOf<DateTimePeriod>()
        private val dateTimePeriodArrayTypeEncoder = arrayTypeEncoder(
            encoder = dateTimePeriodTypeEncoder,
            pgType = PgType.IntervalArray,
        )

        /**
         * Special [PgArrayTypeEncoder] when the array is empty or all items are null. This uses a
         * [PgTypeEncoder] that does nothing since it will never be called.
         */
        private val emptyOrNullOnlyArrayEncoder = arrayTypeEncoder(
            encoder = PgTypeEncoder<Any>(PgType.Unknown) { _, _ -> },
            pgType = PgType.Unknown,
        )

        /** Collection of the default encoders required for all postgresql clients */
        private val defaultEncoders: List<PgTypeEncoder<*>> = listOf(
            booleanTypeEncoder,
            arrayTypeEncoder(booleanTypeEncoder, PgType.BoolArray),
            charTypeEncoder,
            arrayTypeEncoder(charTypeEncoder, PgType.CharArray),
            byteArrayTypeEncoder,
            arrayTypeEncoder(byteArrayTypeEncoder, PgType.ByteaArray),
            shortTypeEncoder,
            arrayTypeEncoder(shortTypeEncoder, PgType.Int2Array),
            intTypeEncoder,
            arrayTypeEncoder(
                encoder = intTypeEncoder,
                pgType = PgType.Int4Array,
                compatibleTypes = arrayOf(PgType.OidArray),
            ),
            longTypeEncoder,
            arrayTypeEncoder(longTypeEncoder, PgType.Int8Array),
            bigDecimalTypeEncoder,
            arrayTypeEncoder(bigDecimalTypeEncoder, PgType.NumericArray),
            floatTypeEncoder,
            arrayTypeEncoder(floatTypeEncoder, PgType.Float4Array),
            doubleTypeEncoder,
            arrayTypeEncoder(doubleTypeEncoder, PgType.Float8Array),
            moneyTypeEncoder,
            arrayTypeEncoder(moneyTypeEncoder, PgType.MoneyArray),
            stringTypeEncoder,
            arrayTypeEncoder(
                encoder = stringTypeEncoder,
                pgType = PgType.TextArray,
                compatibleTypes = arrayOf(
                    PgType.TextArray,
                    PgType.NameArray,
                    PgType.BpcharArray,
                    PgType.VarcharArray,
                ),
            ),
            uuidTypeEncoder,
            arrayTypeEncoder(uuidTypeEncoder, PgType.UuidArray),
            jsonTypeEncoder,
            arrayTypeEncoder(jsonTypeEncoder, PgType.JsonArray),
            inetTypeEncoder,
            arrayTypeEncoder(inetTypeEncoder, PgType.InetArray),
            dateTypeEncoder,
            arrayTypeEncoder(dateTypeEncoder, PgType.DateArray),
            timeTypeEncoder,
            arrayTypeEncoder(timeTypeEncoder, PgType.TimeArray),
            dateTimeTypeEncoder,
            arrayTypeEncoder(
                encoder = dateTimeTypeEncoder,
                pgType = PgType.TimestamptzArray,
            ),
            localDateTimeTypeEncoder,
            arrayTypeEncoder(
                encoder = localDateTimeTypeEncoder,
                pgType = PgType.TimestampArray,
            ),
            timeTzTypeEncoder,
            arrayTypeEncoder(timeTzTypeEncoder, PgType.TimetzArray),
        )

        /** Geometry specific type encoders for databases with the postgis extension installed */
        private val postGisEncoders: List<PgTypeEncoder<*>> = listOf(
            boxTypeEncoder,
            arrayTypeEncoder(boxTypeEncoder, PgType.BoxArray),
            circleTypeEncoder,
            arrayTypeEncoder(circleTypeEncoder, PgType.CircleArray),
            lineTypeEncoder,
            arrayTypeEncoder(lineTypeEncoder, PgType.LineArray),
            lineSegmentTypeEncoder,
            arrayTypeEncoder(lineSegmentTypeEncoder, PgType.LineSegmentArray),
            pathTypeEncoder,
            arrayTypeEncoder(pathTypeEncoder, PgType.PathArray),
            pointTypeEncoder,
            arrayTypeEncoder(pointTypeEncoder, PgType.PointArray),
            polygonTypeEncoder,
            arrayTypeEncoder(polygonTypeEncoder, PgType.PolygonArray),
        )

        private val stringArrayTypeDecoder = arrayTypeDecoder(stringTypeDecoder)
        private val intArrayTypeDecoder = arrayTypeDecoder(intTypeDecoder)
        private val inetArrayTypeDecoder = arrayTypeDecoder(inetTypeDecoder)

        /** Collection of the default decoders required for all postgresql clients */
        private val defaultDecoders = mapOf(
            PgType.VOID to PgTypeDecoder {},
            PgType.BOOL to booleanTypeDecoder,
            PgType.BOOL_ARRAY to arrayTypeDecoder(booleanTypeDecoder),
            PgType.CHAR to charTypeDecoder,
            PgType.CHAR_ARRAY to arrayTypeDecoder(charTypeDecoder),
            PgType.BYTEA to byteArrayTypeDecoder,
            PgType.BYTEA_ARRAY to arrayTypeDecoder(byteArrayTypeDecoder),
            PgType.INT2 to shortTypeDecoder,
            PgType.INT2_ARRAY to arrayTypeDecoder(shortTypeDecoder),
            PgType.INT4 to intTypeDecoder,
            PgType.INT4_ARRAY to intArrayTypeDecoder,
            PgType.INT8 to longTypeDecoder,
            PgType.INT8_ARRAY to arrayTypeDecoder(longTypeDecoder),
            PgType.OID to intTypeDecoder,
            PgType.OID_ARRAY to intArrayTypeDecoder,
            PgType.NUMERIC to bigDecimalTypeDecoder,
            PgType.NUMERIC_ARRAY to arrayTypeDecoder(bigDecimalTypeDecoder),
            PgType.FLOAT4 to floatTypeDecoder,
            PgType.FLOAT4_ARRAY to arrayTypeDecoder(floatTypeDecoder),
            PgType.FLOAT8 to doubleTypeDecoder,
            PgType.FLOAT8_ARRAY to arrayTypeDecoder(doubleTypeDecoder),
            PgType.MONEY to moneyTypeDecoder,
            PgType.MONEY_ARRAY to arrayTypeDecoder(moneyTypeDecoder),
            PgType.TEXT to stringTypeDecoder,
            PgType.TEXT_ARRAY to stringArrayTypeDecoder,
            PgType.BPCHAR to stringTypeDecoder,
            PgType.BPCHAR_ARRAY to stringArrayTypeDecoder,
            PgType.VARCHAR to stringTypeDecoder,
            PgType.VARCHAR_ARRAY to stringArrayTypeDecoder,
            PgType.NAME to stringTypeDecoder,
            PgType.NAME_ARRAY to stringArrayTypeDecoder,
            PgType.UUID to uuidTypeDecoder,
            PgType.UUID_ARRAY to arrayTypeDecoder(uuidTypeDecoder),
            PgType.JSON to jsonTypeDecoder,
            PgType.JSON_ARRAY to arrayTypeDecoder(jsonTypeDecoder),
            PgType.JSONB to jsonTypeDecoder,
            PgType.JSONB_ARRAY to arrayTypeDecoder(jsonTypeDecoder),
            PgType.XML to stringTypeDecoder,
            PgType.XML_ARRAY to stringArrayTypeDecoder,
            PgType.INET to inetTypeDecoder,
            PgType.INET_ARRAY to inetArrayTypeDecoder,
            PgType.CIDR to inetTypeDecoder,
            PgType.CIDR_ARRAY to inetArrayTypeDecoder,
            PgType.DATE to dateTypeDecoder,
            PgType.DATE_ARRAY to arrayTypeDecoder(dateTypeDecoder),
            PgType.TIME to timeTypeDecoder,
            PgType.TIME_ARRAY to arrayTypeDecoder(timeTypeDecoder),
            PgType.TIMESTAMP to localDateTimeTypeDecoder,
            PgType.TIMESTAMP_ARRAY to arrayTypeDecoder(localDateTimeTypeDecoder),
            PgType.TIMETZ to timeTzTypeDecoder,
            PgType.TIMETZ_ARRAY to arrayTypeDecoder(timeTzTypeDecoder),
            PgType.TIMESTAMPTZ to dateTimeTypeDecoder,
            PgType.TIMESTAMPTZ_ARRAY to arrayTypeDecoder(dateTimeTypeDecoder),
            PgType.INTERVAL to dateTimePeriodTypeDecoder,
            PgType.INTERVAL_ARRAY to arrayTypeDecoder(dateTimePeriodTypeDecoder),
        )

        /** Geometry specific type decoders for databases with the postgis extension installed */
        private val postGisDecoders: Map<Int, PgTypeDecoder<*>> = mapOf(
            PgType.BOX to boxTypeDecoder,
            PgType.BOX_ARRAY to arrayTypeDecoder(boxTypeDecoder),
            PgType.CIRCLE to circleTypeDecoder,
            PgType.CIRCLE_ARRAY to arrayTypeDecoder(circleTypeDecoder),
            PgType.LINE to lineTypeDecoder,
            PgType.LINE_ARRAY to arrayTypeDecoder(lineTypeDecoder),
            PgType.LSEG to lineSegmentTypeDecoder,
            PgType.LSEG_ARRAY to arrayTypeDecoder(lineSegmentTypeDecoder),
            PgType.PATH to pathTypeDecoder,
            PgType.PATH_ARRAY to arrayTypeDecoder(pathTypeDecoder),
            PgType.POINT to pointTypeDecoder,
            PgType.POINT_ARRAY to arrayTypeDecoder(pointTypeDecoder),
            PgType.POLYGON to polygonTypeDecoder,
            PgType.POLYGON_ARRAY to arrayTypeDecoder(polygonTypeDecoder),
        )

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

        /** Query to fetch the OID of the array type with an inner type matching the OID supplied */
        private val pgArrayTypeByInnerOid =
            """
            select typarray
            from pg_type
            where oid = $1
            """.trimIndent()

        /**
         * Fetch and return the array OID for a type whose inner [oid] is specified. Queries the
         * database using the [connection] provided to retrieve the database instance specific OID.
         * Returns null if the OID could not be found.
         */
        private suspend fun checkArrayDbTypeByOid(oid: Int, connection: PgConnection): Int? {
            val arrayOid = connection.sendPreparedStatement(
                query = pgArrayTypeByInnerOid,
                parameters = listOf(oid)
            ).use {
                val result = it.firstOrNull()
                    ?: error("Found no results when executing a check for array db type by oid")
                result.rows.firstOrNull()?.getInt(0)
            }

            if (arrayOid == null) {
                logger.atWarn {
                    message = "Could not find array type for oid = {oid}"
                    payload = mapOf("oid" to oid)
                }
                return null
            }
            return arrayOid
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
            name: String,
            connection: PgConnection,
        ): Int? {
            var schema: String? = null
            var typeName = name
            val schemaQualifierIndex = name.indexOf('.')
            if (schemaQualifierIndex > -1) {
                schema = name.substring(0, schemaQualifierIndex)
                typeName = name.substring(schemaQualifierIndex + 1)
            }

            val parameters = listOf(typeName, schema)
            val oid = connection.sendPreparedStatement(pgEnumTypeByName, parameters).use {
                val result = it.firstOrNull()
                    ?: error("Found no results when executing a check for enum db type by name")
                result.rows.firstOrNull()?.getInt(0)
            }
            if (oid == null) {
                logger.atWarn {
                    message = "Could not find enum type for name = {name}"
                    payload = mapOf("name" to name)
                }
                return null
            }
            return oid
        }

        /**
         * Fetch and return the type OID for a composite with the [name]. Queries the database
         * using the [connection] provided to retrieve the database instance specific OID. Returns
         * null if the OID could not be found.
         *
         * @param name Name of the composite type. Can be schema qualified but defaults to public
         * if no schema is included
         */
        private suspend fun checkCompositeDbTypeByName(
            name: String,
            connection: PgConnection,
        ): Int? {
            var schema: String? = null
            var typeName = name
            val schemaQualifierIndex = name.indexOf('.')
            if (schemaQualifierIndex > -1) {
                schema = name.substring(0, schemaQualifierIndex)
                typeName = name.substring(schemaQualifierIndex + 1)
            }

            val parameters = listOf(typeName, schema)
            val oid = connection.sendPreparedStatement(pgCompositeTypeByName, parameters).use {
                val result = it.firstOrNull() ?: error(
                    "Found no results when executing a check for composite db type by name"
                )
                result.rows.firstOrNull()?.getInt(0)
            }
            if (oid == null) {
                logger.atWarn {
                    message = "Could not find composite type for name = {name}"
                    payload = mapOf("name" to name)
                }
                return null
            }
            return oid
        }

        /**
         * Fetch and return the array OID for a type whose inner [oid] is specified. Queries the
         * database using the [connection] provided to retrieve the database instance specific OID.
         * Returns null if the OID could not be found.
         */
        private fun checkArrayDbTypeByOid(oid: Int, connection: PgBlockingConnection): Int? {
            val arrayOid = connection.sendPreparedStatement(
                query = pgArrayTypeByInnerOid,
                parameters = listOf(oid)
            ).use {
                val result = it.firstOrNull()
                    ?: error("Found no results when executing a check for array db type by oid")
                result.rows.firstOrNull()?.getInt(0)
            }

            if (arrayOid == null) {
                logger.atWarn {
                    message = "Could not find array type for oid = {oid}"
                    payload = mapOf("oid" to oid)
                }
                return null
            }
            return arrayOid
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
            name: String,
            connection: PgBlockingConnection,
        ): Int? {
            var schema: String? = null
            var typeName = name
            val schemaQualifierIndex = name.indexOf('.')
            if (schemaQualifierIndex > -1) {
                schema = name.substring(0, schemaQualifierIndex)
                typeName = name.substring(schemaQualifierIndex + 1)
            }

            val parameters = listOf(typeName, schema)
            val oid = connection.sendPreparedStatement(pgEnumTypeByName, parameters).use {
                val result = it.firstOrNull()
                    ?: error("Found no results when executing a check for enum db type by name")
                result.rows.firstOrNull()?.getInt(0)
            }
            if (oid == null) {
                logger.atWarn {
                    message = "Could not find enum type for name = {name}"
                    payload = mapOf("name" to name)
                }
                return null
            }
            return oid
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
            name: String,
            connection: PgBlockingConnection,
        ): Int? {
            var schema: String? = null
            var typeName = name
            val schemaQualifierIndex = name.indexOf('.')
            if (schemaQualifierIndex > -1) {
                schema = name.substring(0, schemaQualifierIndex)
                typeName = name.substring(schemaQualifierIndex + 1)
            }

            val parameters = listOf(typeName, schema)
            val oid = connection.sendPreparedStatement(pgCompositeTypeByName, parameters).use {
                val result = it.firstOrNull() ?: error(
                    "Found no results when executing a check for composite db type by name"
                )
                result.rows.firstOrNull()?.getInt(0)
            }
            if (oid == null) {
                logger.atWarn {
                    message = "Could not find composite type for name = {name}"
                    payload = mapOf("name" to name)
                }
                return null
            }
            return oid
        }
    }
}
