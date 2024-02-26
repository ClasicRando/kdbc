package com.github.clasicrando.postgresql.column

import com.github.clasicrando.common.buffer.ByteWriteBuffer
import com.github.clasicrando.common.use
import com.github.clasicrando.postgresql.connection.PgConnection
import io.github.oshai.kotlinlogging.KotlinLogging
import kotlinx.datetime.DateTimePeriod
import java.util.concurrent.ConcurrentHashMap
import kotlin.reflect.KType
import kotlin.reflect.KTypeProjection
import kotlin.reflect.full.createType
import kotlin.reflect.full.isSubtypeOf
import kotlin.reflect.typeOf

@PublishedApi
internal val typeRegistryLogger = KotlinLogging.logger {}

@PublishedApi
internal class PgTypeRegistry {
    private val encoders: MutableMap<KType, PgTypeEncoder<*>> = ConcurrentHashMap(defaultEncoders.associateBy { it.encodeType })
    private val decoders: MutableMap<Int, PgTypeDecoder<*>> = ConcurrentHashMap(defaultDecoders)
    private var hasPostGisTypes = false

    fun decode(value: PgValue): Any {
        val oid = value.typeData.dataType
        return decoders[oid]
            ?.decode(value)
            ?: error("Could not find decoder when looking up oid = $oid")
    }

    fun encode(value: Any, buffer: ByteWriteBuffer) {
        return when (value) {
            is DateTimePeriod -> dateTimePeriodTypeEncoder.encode(value, buffer)
            is List<*> -> encodeList(value, buffer)
            else -> encodeInternal(value, buffer)
        }
    }

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

    @Suppress("UNCHECKED_CAST")
    private fun <T : Any> encodeInternal(value: T, buffer: ByteWriteBuffer) {
        val type = value::class.createType()
        val encoder = encoders[type]
            ?: error("Could not find encoder when looking up type = $type")
        (encoder as PgTypeEncoder<T>).encode(value, buffer)
    }

    internal fun kindOf(type: KType): PgType {
        val pgType = encoders[type]?.pgType
        if (pgType != null) {
            return pgType
        }
        return when {
            !type.isSubtypeOf(dateTimePeriodType) -> {
                error("Could not find type Oid looking up type = $type")
            }
            else -> dateTimePeriodTypeEncoder.pgType
        }
    }

    private fun <T> kindOf(value: List<T>): PgType {
        if (value.isEmpty() || value.firstNotNullOfOrNull { it } == null) {
            return PgType.Unspecified
        }
        val elementType = value[0]!!::class.createType(nullable = true)
        if (elementType.isSubtypeOf(dateTimePeriodType)) {
            return dateTimePeriodArrayTypeEncoder.pgType
        }
        val typeParameter = KTypeProjection.invariant(elementType)
        val type = List::class.createType(arguments = listOf(typeParameter))
        return kindOf(type)
    }

    fun kindOf(value: Any?): PgType {
        return try {
            when (value) {
                null -> PgType.Unspecified
                is List<*> -> kindOf(value)
                else -> kindOf(value::class.createType())
            }
        } catch (ex: Throwable) {
            PgType.Unspecified
        }
    }

    fun includePostGisTypes() {
        if (hasPostGisTypes) {
            return
        }
        for (encoder in postGisEncoders) {
            encoders[encoder.encodeType] = encoder
        }
        for ((oid, decoder) in postGisDecoders) {
            decoders[oid] = decoder
        }
        hasPostGisTypes = true
    }

    @PublishedApi
    internal fun checkAgainstExistingType(type: KType, oid: Int) {
        if (encoders.containsKey(type) || decoders.containsKey(oid)) {
            typeRegistryLogger.atWarn {
                message = "Replacing type definition for type = {type}"
                payload = mapOf("type" to type)
            }
        }
    }

    @PublishedApi
    internal fun <E : Any, D : Any> addTypeToCaches(
        oid: Int,
        encoder: PgTypeEncoder<E>,
        decoder: PgTypeDecoder<D>,
    ) {
        encoders[encoder.encodeType] = encoder
        decoders[oid] = decoder
    }

    suspend fun <E : Enum<E>, D : Enum<D>> registerEnumType(
        connection: PgConnection,
        encoder: PgTypeEncoder<E>,
        enumType: KType,
        decoder: PgTypeDecoder<D>,
        type: String,
    ) {
        val verifiedOid = checkEnumDbTypeByName(type, connection)
            ?: error("Could not verify the enum type name '$type' in the database")
        typeRegistryLogger.atTrace {
            message = "Adding column decoder for enum type {name} ({oid})"
            payload = mapOf("name" to type, "oid" to verifiedOid)
        }
        checkAgainstExistingType(encoder.encodeType, verifiedOid)
        encoder.pgType = PgType.ByName(type, verifiedOid)
        addTypeToCaches(verifiedOid, encoder, decoder)

        val arrayOid = checkArrayDbTypeByOid(verifiedOid, connection)
            ?: error("Could not verify the array type for element oid = $verifiedOid")
        typeRegistryLogger.atTrace {
            message = "Adding array column decoder for enum type {name} ({oid})"
            payload = mapOf("name" to type, "oid" to verifiedOid)
        }
        val arrayTypeEncoder = arrayTypeEncoder(
            encoder = encoder,
            pgType = PgType.ByName("_$type", arrayOid),
            arrayType = enumType,
        )
        checkAgainstExistingType(arrayTypeEncoder.encodeType, arrayOid)
        addTypeToCaches(
            oid = arrayOid,
            encoder = arrayTypeEncoder,
            decoder = arrayTypeDecoder(decoder, enumType),
        )
    }

    @PublishedApi
    internal suspend fun <E : Any, D : Any> registerCompositeType(
        connection: PgConnection,
        encoder: PgTypeEncoder<E>,
        decoder: PgTypeDecoder<D>,
        type: String,
        arrayType: KType,
    ) {
        val verifiedOid = checkCompositeDbTypeByName(type, connection)
            ?: error("Could not verify the composite type name '$type' in the database")
        typeRegistryLogger.atTrace {
            message = "Adding column decoder for composite type {name} ({oid})"
            payload = mapOf("name" to type, "oid" to verifiedOid)
        }
        encoder.pgType = PgType.ByName(type, verifiedOid)
        checkAgainstExistingType(encoder.encodeType, verifiedOid)
        addTypeToCaches(verifiedOid, encoder, decoder)

        val arrayOid = checkArrayDbTypeByOid(verifiedOid, connection)
            ?: error("Could not verify the array type for element oid = $verifiedOid")
        typeRegistryLogger.atTrace {
            message = "Adding array column decoder for composite type {name} ({oid})"
            payload = mapOf("name" to type, "oid" to verifiedOid)
        }
        val arrayTypeEncoder = arrayTypeEncoder(
            encoder = encoder,
            pgType = PgType.ByName("_$type", arrayOid),
            arrayType = arrayType,
        )
        checkAgainstExistingType(arrayTypeEncoder.encodeType, arrayOid)
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

        private val emptyOrNullOnlyArrayEncoder = arrayTypeEncoder(
            encoder = PgTypeEncoder<Any>(PgType.Unknown) { _, _ -> },
            pgType = PgType.Unknown,
        )

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
            PgType.INET_ARRAY to arrayTypeDecoder(inetTypeDecoder),
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
        private val pgArrayTypeByInnerOid =
            """
            select typarray
            from pg_type
            where oid = $1
            """.trimIndent()

        @PublishedApi
        internal suspend fun checkArrayDbTypeByOid(oid: Int, connection: PgConnection): Int? {
            val arrayOid = connection.sendPreparedStatement(
                query = pgArrayTypeByInnerOid,
                parameters = listOf(oid)
            ).use {
                val result = it.firstOrNull()
                    ?: error("Found no results when executing a check for array db type by oid")
                result.rows.firstOrNull()?.getInt(0)
            }

            if (arrayOid == null) {
                typeRegistryLogger.atWarn {
                    message = "Could not find array type for oid = {oid}"
                    payload = mapOf("oid" to oid)
                }
                return null
            }
            return arrayOid
        }

        @PublishedApi
        internal suspend fun checkEnumDbTypeByName(
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
                typeRegistryLogger.atWarn {
                    message = "Could not find enum type for name = {name}"
                    payload = mapOf("name" to name)
                }
                return null
            }
            return oid
        }

        @PublishedApi
        internal suspend fun checkCompositeDbTypeByName(
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
                typeRegistryLogger.atWarn {
                    message = "Could not find composite type for name = {name}"
                    payload = mapOf("name" to name)
                }
                return null
            }
            return oid
        }
    }
}
