package com.github.clasicrando.postgresql.column

import com.github.clasicrando.common.result.getInt
import com.github.clasicrando.postgresql.connection.PgConnection
import com.github.clasicrando.postgresql.statement.PgArguments
import io.github.oshai.kotlinlogging.KotlinLogging
import java.util.concurrent.ConcurrentHashMap
import kotlin.reflect.KType
import kotlin.reflect.full.createType

@PublishedApi
internal val typeRegistryLogger = KotlinLogging.logger {}

internal class PgTypeRegistry {
    private val encoders: MutableMap<KType, PgTypeEncoder<*>> = ConcurrentHashMap(defaultEncoders.associateBy { it.encodeType })
    private val decoders: MutableMap<Int, PgTypeDecoder<*>> = ConcurrentHashMap(defaultDecoders)

    fun decode(value: PgValue): Any {
        val oid = value.typeData.dataType
        return decoders[oid]
            ?.decode(value)
            ?: error("Could not find decoder when looking up oid = $oid")
    }

    fun encode(value: Any?, buffer: PgArguments) {
        if (value == null) {
            buffer.writeByte(-1)
            return
        }
        return encodeInternal(value, buffer)
    }

    @Suppress("UNCHECKED_CAST")
    private fun <T : Any> encodeInternal(value: T, buffer: PgArguments) {
        val type = value::class.createType()
        val encoder = encoders[type]
            ?: error("Could not find encoder when looking up type = $type")
        (encoder as PgTypeEncoder<T>).encode(value, buffer)
    }

    fun kindOf(type: KType): PgType {
        return encoders[type]?.pgType ?: error("Could not find type Oid looking up type = $type")
    }

    fun kindOf(value: Any?): PgType {
        if (value == null) {
            return PgType.Unknown
        }
        return kindOf(value::class.createType())
    }

    @PublishedApi
    internal fun checkAgainstExistingType(type: KType, oid: Int) {
        if (encoders.containsKey(type) || decoders.containsKey(oid)) {
            typeRegistryLogger.atWarn {
                message = "Replacing default type definition for type = {type}"
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

    /**
     * Register a new decoder with the specified [type] as it's type oid. You should prefer the
     * other [registerType] method that accepts a type name since a non-standard type's oid
     * might vary between database instances.
     *
     * Note, if you provide a type oid that belongs to a built-in type, this new type definition
     * will override the default and possibly cause the decoding and encoding of values to fail
     */
    suspend inline fun <reified E : Any, reified D : Any> PgConnection.registerType(
        type: Int,
        encoder: PgTypeEncoder<E>,
        decoder: PgTypeDecoder<D>,
    ) {
        val verifiedOid = checkDbTypeByOid(type, this)
            ?: error("Could not verify the type name '$type' in the database")
        typeRegistryLogger.atTrace {
            message = "Adding column decoder for type {name} ({oid})"
            payload = mapOf("oid" to verifiedOid)
        }
        checkAgainstExistingType(encoder.encodeType, verifiedOid)
        addTypeToCaches(verifiedOid, encoder, decoder)

        val arrayOid = checkArrayDbTypeByOid(verifiedOid, this)
            ?: error("Could not verify the array type for element oid = $verifiedOid")
        typeRegistryLogger.atTrace {
            message = "Adding array column decoder for type {name} ({oid})"
            payload = mapOf("oid" to verifiedOid)
        }
        val arrayTypeEncoder = arrayTypeEncoder(encoder, PgType.ByOid(arrayOid))
        checkAgainstExistingType(arrayTypeEncoder.encodeType, arrayOid)
        addTypeToCaches(
            oid = verifiedOid,
            encoder = arrayTypeEncoder,
            decoder = arrayTypeDecoder(decoder),
        )
    }

    /**
     * Register a new decoder with the specified [type] name. You should prefer this method over
     * the other [registerType] method since it allows the connection cache the oid on
     * startup and always match your database instance.
     *
     * Note, if you provide a type oid that belongs to a built-in type, this new type definition
     * will override the default and possibly cause the decoding and encoding of values to fail
     */
    suspend inline fun <reified E : Any, reified D : Any> PgConnection.registerType(
        type: String,
        encoder: PgTypeEncoder<E>,
        decoder: PgTypeDecoder<D>,
    ) {
        val verifiedOid = checkDbTypeByName(type, this)
            ?: error("Could not verify the type name '$type' in the database")
        typeRegistryLogger.atTrace {
            message = "Adding column decoder for type {name} ({oid})"
            payload = mapOf("oid" to verifiedOid)
        }
        checkAgainstExistingType(encoder.encodeType, verifiedOid)
        addTypeToCaches(verifiedOid, encoder, decoder)

        val arrayOid = checkArrayDbTypeByOid(verifiedOid, this)
            ?: error("Could not verify the array type for element oid = $verifiedOid")
        typeRegistryLogger.atTrace {
            message = "Adding array column decoder for type {name} ({oid})"
            payload = mapOf("oid" to verifiedOid)
        }
        val arrayTypeEncoder = arrayTypeEncoder(encoder, PgType.ByName("_$type", arrayOid))
        checkAgainstExistingType(arrayTypeEncoder.encodeType, arrayOid)
        addTypeToCaches(
            oid = verifiedOid,
            encoder = arrayTypeEncoder,
            decoder = arrayTypeDecoder(decoder),
        )
    }

    suspend inline fun <reified E : Enum<E>, reified D : Enum<D>> PgConnection.registerEnumType(
        encoder: PgTypeEncoder<E>,
        decoder: PgTypeDecoder<D>,
        type: String,
    ) {
        val verifiedOid = checkEnumDbTypeByName(type, this)
            ?: error("Could not verify the enum type name '$type' in the database")
        typeRegistryLogger.atTrace {
            message = "Adding column decoder for enum type {name} ({oid})"
            payload = mapOf("name" to type, "oid" to verifiedOid)
        }
        checkAgainstExistingType(encoder.encodeType, verifiedOid)
        addTypeToCaches(verifiedOid, encoder, decoder)

        val arrayOid = checkArrayDbTypeByOid(verifiedOid, this)
            ?: error("Could not verify the array type for element oid = $verifiedOid")
        typeRegistryLogger.atTrace {
            message = "Adding array column decoder for enum type {name} ({oid})"
            payload = mapOf("name" to type, "oid" to verifiedOid)
        }
        val arrayTypeEncoder = arrayTypeEncoder(encoder, PgType.ByName("_$type", arrayOid))
        checkAgainstExistingType(arrayTypeEncoder.encodeType, arrayOid)
        addTypeToCaches(
            oid = arrayOid,
            encoder = arrayTypeEncoder,
            decoder = arrayTypeDecoder(decoder),
        )
    }

    suspend inline fun <reified E : Enum<E>> PgConnection.registerEnumType(type: String) {
        registerEnumType(enumTypeEncoder<E>(type), enumTypeDecoder<E>(), type)
    }

    suspend inline fun <reified E : Any, reified D : Any> PgConnection.registerCompositeType(
        encoder: PgTypeEncoder<E>,
        decoder: PgTypeDecoder<D>,
        type: String,
    ) {
        val verifiedOid = checkCompositeDbTypeByName(type, this)
            ?: error("Could not verify the composite type name '$type' in the database")
        typeRegistryLogger.atTrace {
            message = "Adding column decoder for composite type {name} ({oid})"
            payload = mapOf("name" to type, "oid" to verifiedOid)
        }
        checkAgainstExistingType(encoder.encodeType, verifiedOid)
        addTypeToCaches(verifiedOid, encoder, decoder)

        val arrayOid = checkArrayDbTypeByOid(verifiedOid, this)
            ?: error("Could not verify the array type for element oid = $verifiedOid")
        typeRegistryLogger.atTrace {
            message = "Adding array column decoder for composite type {name} ({oid})"
            payload = mapOf("name" to type, "oid" to verifiedOid)
        }
        val arrayTypeEncoder = arrayTypeEncoder(encoder, PgType.ByName("_$type", arrayOid))
        checkAgainstExistingType(arrayTypeEncoder.encodeType, arrayOid)
        addTypeToCaches(
            oid = verifiedOid,
            encoder = arrayTypeEncoder,
            decoder = arrayTypeDecoder(decoder),
        )
    }

    suspend inline fun <reified T : Any> PgConnection.registerCompositeType(type: String) {
        val encoder = compositeTypeEncoder<T>(type, this@PgTypeRegistry)
        val decoder = compositeTypeDecoder<T>(this@PgTypeRegistry)
        registerCompositeType(encoder, decoder, type)
    }

    companion object {
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
                compatibleTypes = arrayOf(),
            ),
            timeTzTypeEncoder,
            arrayTypeEncoder(timeTzTypeEncoder, PgType.TimetzArray),
            dateTimePeriodTypeEncoder,
            arrayTypeEncoder(dateTimePeriodTypeEncoder, PgType.IntervalArray),
        )

        private val stringArrayTypeDecoder = arrayTypeDecoder(stringTypeDecoder)
        private val dateTimeArrayTypeDecoder = arrayTypeDecoder(dateTimeTypeDecoder)
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
            PgType.TIMESTAMP to dateTimeTypeDecoder,
            PgType.TIMESTAMP_ARRAY to dateTimeArrayTypeDecoder,
            PgType.TIMETZ to timeTzTypeDecoder,
            PgType.TIMETZ_ARRAY to arrayTypeDecoder(timeTzTypeDecoder),
            PgType.TIMESTAMPTZ to dateTimeTypeDecoder,
            PgType.TIMESTAMPTZ_ARRAY to dateTimeArrayTypeDecoder,
            PgType.INTERVAL to dateTimePeriodTypeDecoder,
            PgType.INTERVAL_ARRAY to arrayTypeDecoder(dateTimePeriodTypeDecoder),
        )

        private val pgTypeByOid =
            """
            select null
            from pg_type
            where oid = $1
            """.trimIndent()
        private val pgTypeByName =
            """
            select t.oid
            from pg_type t
            join pg_namespace n on t.typnamespace = n.oid
            where
                t.typname = $1
                and n.nspname = coalesce(nullif($2,''), 'public')
            """.trimIndent()
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
        internal suspend fun checkDbTypeByOid(oid: Int, connection: PgConnection): Int? {
            val result = connection.sendPreparedStatement(pgTypeByOid, listOf(oid))
                .firstOrNull()
                ?: error("Found no results when executing a check for db type by oid")
            if (result.rowsAffected == 0L) {
                typeRegistryLogger.atWarn {
                    message = "Could not find type for oid = {oid}"
                    payload = mapOf("oid" to oid)
                }
                return null
            }
            return oid
        }

        @PublishedApi
        internal suspend fun checkArrayDbTypeByOid(oid: Int, connection: PgConnection): Int? {
            val result = connection.sendPreparedStatement(pgArrayTypeByInnerOid, listOf(oid))
                .firstOrNull()
                ?: error("Found no results when executing a check for array db type by oid")
            if (result.rowsAffected == 0L) {
                typeRegistryLogger.atWarn {
                    message = "Could not find array type for oid = {oid}"
                    payload = mapOf("oid" to oid)
                }
                return null
            }
            return result.rows.firstOrNull()?.getInt(0)
        }

        @PublishedApi
        internal suspend fun checkDbTypeByName(name: String, connection: PgConnection): Int? {
            var schema: String? = null
            var typeName = name
            val schemaQualifierIndex = name.indexOf('.')
            if (schemaQualifierIndex > -1) {
                schema = name.substring(0, schemaQualifierIndex)
                typeName = name.substring(schemaQualifierIndex + 1)
            }

            val result = connection.sendPreparedStatement(
                pgTypeByName,
                listOf(typeName, schema),
            ).firstOrNull() ?: error("Found no results when executing a check for db type by name")
            val oid = result.rows.firstOrNull()?.getInt(0)
            if (oid == null) {
                typeRegistryLogger.atWarn {
                    message = "Could not find type for name = {name}"
                    payload = mapOf("name" to name)
                }
                return null
            }
            return oid
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
            val result = connection.sendPreparedStatement(pgEnumTypeByName, parameters)
                .firstOrNull()
                ?: error("Found no results when executing a check for enum db type by name")
            val oid = result.rows.firstOrNull()?.getInt(0)
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
            val result = connection.sendPreparedStatement(pgCompositeTypeByName, parameters)
                .firstOrNull()
                ?: error("Found no results when executing a check for composite db type by name")
            val oid = result.rows.firstOrNull()?.getInt(0)
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
