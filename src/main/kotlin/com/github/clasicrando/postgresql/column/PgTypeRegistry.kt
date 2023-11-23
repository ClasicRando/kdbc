package com.github.clasicrando.postgresql.column

import com.github.clasicrando.common.atomic.AtomicMutableMap
import com.github.clasicrando.common.column.BigDecimalDbType
import com.github.clasicrando.common.column.ColumnData
import com.github.clasicrando.common.column.DateTimeDbType
import com.github.clasicrando.common.column.DateTimePeriodDbType
import com.github.clasicrando.common.column.DbType
import com.github.clasicrando.common.column.DoubleDbType
import com.github.clasicrando.common.column.FloatDbType
import com.github.clasicrando.common.column.IntDbType
import com.github.clasicrando.common.column.LocalDateDbType
import com.github.clasicrando.common.column.LocalTimeDbType
import com.github.clasicrando.common.column.LongDbType
import com.github.clasicrando.common.column.ShortDbType
import com.github.clasicrando.common.column.StringDbType
import com.github.clasicrando.common.column.TypeRegistry
import com.github.clasicrando.common.column.UuidDbType
import com.github.clasicrando.postgresql.PgConnection
import com.github.clasicrando.postgresql.array.ArrayType
import com.github.clasicrando.postgresql.type.enumDbType
import io.klogging.Klogging
import io.ktor.utils.io.charsets.Charset
import kotlinx.coroutines.sync.Mutex

private typealias DbTypeDefinition = Pair<DbType, Boolean>

class PgTypeRegistry(
    private val nonStandardTypesByOid: Map<Int, DbTypeDefinition> = Companion.nonStandardTypesByOid.toMap(),
    private val nonStandardTypesByName: Map<String, DbTypeDefinition> = Companion.nonStandardTypesByName.toMap(),
    private val enumTypes: Map<String, DbTypeDefinition> = Companion.enumTypes.toMap(),
) : TypeRegistry {
    private var types = defaultTypes
    private var classToType = types.entries
        .asSequence()
        .filter { it.value !is ArrayType }
        .associateBy { it.value.encodeType }

    override fun decode(type: ColumnData, value: ByteArray, charset: Charset): Any {
        return types.getOrDefault(type.dataType, StringDbType)
            .decode(type, value, charset)
    }

    private fun encodeArray(array: Iterable<*>): String {
        return array.joinToString(
            separator = ",",
            prefix = "{",
            postfix = "}",
        ) {  item ->
            if (item == null) {
                return@joinToString "NULL"
            }

            val encodedValue = encode(item)!!
            if (needQuote(item)) {
                val escapedValue = encodedValue.replace("\\", """\\""")
                    .replace("\"", """\"""")
                "\"$escapedValue\""
            } else {
                encodedValue
            }
        }
    }

    private fun needQuote(value: Any): Boolean {
        return when (value) {
            is Number, is Iterable<*>, is Array<*> -> false
            else -> true
        }
    }

    override fun encode(value: Any?): String? {
        if (value == null) {
            return null
        }
        return when (value) {
            is Array<*> -> encodeArray(value.asIterable())
            is Iterable<*> -> encodeArray(value)
            else -> classToType[value::class]
                ?.value
                ?.encode(value)
                ?: StringDbType.encode(value)
        }
    }

    override fun kindOf(value: Any?): Int {
        return when (value) {
            null -> 0
            is String -> PgColumnTypes.Untyped
            else -> classToType[value::class]?.key ?: PgColumnTypes.Untyped
        }
    }
    
    suspend fun finalizeTypes(connection: PgConnection) {
        val nonStandardTypes = buildMap {
            for ((oid, pair) in nonStandardTypesByOid) {
                val (dbType, hasArray) = pair
                val verifiedOid = checkDbTypeByOid(oid, connection) ?: continue
                logger.info("Adding column decoder for type {oid}", verifiedOid)
                checkAgainstDefaultTypes(verifiedOid)
                this[verifiedOid] = dbType

                if (hasArray) {
                    checkArrayDbTypeByOid(verifiedOid, connection)?.let {
                        logger.info(
                            "Adding array column decoder for type {oid}",
                            verifiedOid,
                        )
                        checkAgainstDefaultTypes(it)
                        this[it] = ArrayType(dbType)
                    }
                }
            }

            for ((name, pair) in nonStandardTypesByName) {
                val (dbType, hasArray) = pair
                val verifiedOid = checkDbTypeByName(name, connection) ?: continue
                logger.info(
                    "Adding column decoder for type {name} ({oid})",
                    name,
                    verifiedOid,
                )
                checkAgainstDefaultTypes(verifiedOid)
                this[verifiedOid] = dbType

                if (hasArray) {
                    checkArrayDbTypeByOid(verifiedOid, connection)?.let {
                        logger.info(
                            "Adding array column decoder for type {name} ({oid})",
                            name,
                            verifiedOid,
                        )
                        checkAgainstDefaultTypes(it)
                        this[it] = ArrayType(dbType)
                    }
                }
            }

            for ((name, pair) in enumTypes) {
                val (dbType, hasArray) = pair
                val verifiedOid = checkEnumDbTypeByName(name, connection) ?: continue
                logger.info(
                    "Adding column decoder for enum type {name} ({oid})",
                    name,
                    verifiedOid,
                )
                checkAgainstDefaultTypes(verifiedOid)
                this[verifiedOid] = dbType

                if (hasArray) {
                    checkArrayDbTypeByOid(verifiedOid, connection)?.let {
                        logger.info(
                            "Adding array column decoder for enum type {name} ({oid})",
                            name,
                            verifiedOid,
                        )
                        checkAgainstDefaultTypes(it)
                        this[it] = ArrayType(dbType)
                    }
                }
            }
        }
        types = defaultTypes.plus(nonStandardTypes)
        classToType = types.entries
            .asSequence()
            .filter { it.value !is ArrayType }
            .associateBy { it.value.encodeType }
    }

    companion object : Klogging {
        private val stringArrayType = ArrayType(StringDbType)
        private val dateTimeArrayType = ArrayType(DateTimeDbType)

        private val defaultTypes: Map<Int, DbType> get() = mapOf(
            PgColumnTypes.Boolean to PgBooleanDbType,
            PgColumnTypes.BooleanArray to ArrayType(PgBooleanDbType),
            PgColumnTypes.Char to PgCharDbType,
            PgColumnTypes.CharArray to ArrayType(PgCharDbType),
            PgColumnTypes.ByteA to PgByteArrayDbType,
            PgColumnTypes.ByteA_Array to ArrayType(PgByteArrayDbType),
            PgColumnTypes.Smallint to ShortDbType,
            PgColumnTypes.SmallintArray to ArrayType(ShortDbType),
            PgColumnTypes.Integer to IntDbType,
            PgColumnTypes.IntegerArray to ArrayType(IntDbType),
            PgColumnTypes.Bigint to LongDbType,
            PgColumnTypes.BigintArray to ArrayType(LongDbType),
            PgColumnTypes.OID to PgOidDbType,
            PgColumnTypes.OIDArray to ArrayType(PgOidDbType),
            PgColumnTypes.Numeric to BigDecimalDbType,
            PgColumnTypes.NumericArray to ArrayType(BigDecimalDbType),
            PgColumnTypes.Real to FloatDbType,
            PgColumnTypes.RealArray to ArrayType(FloatDbType),
            PgColumnTypes.Double to DoubleDbType,
            PgColumnTypes.DoubleArray to ArrayType(DoubleDbType),
            PgColumnTypes.Money to PgMoneyDbType,
            PgColumnTypes.MoneyArray to ArrayType(PgMoneyDbType),
            PgColumnTypes.Text to StringDbType,
            PgColumnTypes.TextArray to stringArrayType,
            PgColumnTypes.Bpchar to StringDbType,
            PgColumnTypes.BpcharArray to stringArrayType,
            PgColumnTypes.Varchar to StringDbType,
            PgColumnTypes.VarcharArray to stringArrayType,
            PgColumnTypes.CharacterData to StringDbType,
            PgColumnTypes.CharacterDataArray to stringArrayType,
            PgColumnTypes.Name to StringDbType,
            PgColumnTypes.NameArray to stringArrayType,
            PgColumnTypes.UUID to UuidDbType,
            PgColumnTypes.UUIDArray to ArrayType(UuidDbType),
            PgColumnTypes.Json to PgJsonDbType,
            PgColumnTypes.JsonArray to ArrayType(PgJsonDbType),
            PgColumnTypes.Jsonb to PgJsonDbType,
            PgColumnTypes.JsonbArray to ArrayType(PgJsonDbType),
            PgColumnTypes.XML to StringDbType,
            PgColumnTypes.XMLArray to stringArrayType,
            PgColumnTypes.Inet to PgInetDbType,
            PgColumnTypes.InetArray to ArrayType(PgInetDbType),
            PgColumnTypes.Date to LocalDateDbType,
            PgColumnTypes.DateArray to ArrayType(LocalDateDbType),
            PgColumnTypes.Time to LocalTimeDbType,
            PgColumnTypes.TimeArray to ArrayType(LocalTimeDbType),
            PgColumnTypes.Timestamp to DateTimeDbType,
            PgColumnTypes.TimestampArray to dateTimeArrayType,
            PgColumnTypes.TimeWithTimezone to PgTimeTzDbType,
            PgColumnTypes.TimeWithTimezoneArray to ArrayType(PgTimeTzDbType),
            PgColumnTypes.TimestampWithTimezone to DateTimeDbType,
            PgColumnTypes.TimestampWithTimezoneArray to dateTimeArrayType,
            PgColumnTypes.Interval to DateTimePeriodDbType,
            PgColumnTypes.IntervalArray to ArrayType(DateTimePeriodDbType),
        )
        private var instance: PgTypeRegistry? = null
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
        private val pgArrayTypeByInnerOid =
            """
            select typarray
            from pg_type
            where oid = $1
            """.trimIndent()

        private suspend fun checkDbTypeByOid(oid: Int, connection: PgConnection): Int? {
            val result = connection.sendPreparedStatement(pgTypeByOid, listOf(oid))
            if (result.rowsAffected == 0L) {
                logger.warn("Could not find type for oid = {oid}", oid)
                return null
            }
            return oid
        }

        private suspend fun checkArrayDbTypeByOid(oid: Int, connection: PgConnection): Int? {
            val result = connection.sendPreparedStatement(pgArrayTypeByInnerOid, listOf(oid))
            if (result.rowsAffected == 0L) {
                logger.warn("Could not find array type for oid = {oid}", oid)
                return null
            }
            return result.rows.firstOrNull()?.getInt(0)
        }

        private suspend fun checkDbTypeByName(name: String, connection: PgConnection): Int? {
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
            )
            val oid = result.rows.firstOrNull()?.getInt(0)
            if (oid == null) {
                logger.warn("Could not find type for name = {name}", name)
                return null
            }
            return oid
        }

        private suspend fun checkEnumDbTypeByName(name: String, connection: PgConnection): Int? {
            var schema: String? = null
            var typeName = name
            val schemaQualifierIndex = name.indexOf('.')
            if (schemaQualifierIndex > -1) {
                schema = name.substring(0, schemaQualifierIndex)
                typeName = name.substring(schemaQualifierIndex + 1)
            }

            val result = connection.sendPreparedStatement(
                pgEnumTypeByName,
                listOf(typeName, schema),
            )
            val oid = result.rows.firstOrNull()?.getInt(0)
            if (oid == null) {
                logger.warn("Could not find enum type for name = {name}", name)
                return null
            }
            return oid
        }

        private suspend fun checkAgainstDefaultTypes(type: Int) {
            if (defaultTypes.containsKey(type)) {
                logger.info("Replacing default type definition for type = {type}", type)
            }
        }

        private val nonStandardTypesByOid: MutableMap<Int, DbTypeDefinition> = AtomicMutableMap()
        private val nonStandardTypesByName: MutableMap<String, DbTypeDefinition> = AtomicMutableMap()
        private val enumTypes: MutableMap<String, DbTypeDefinition> = AtomicMutableMap()

        /**
         * Register a new decoder with the specified [type] as it's type oid. You should prefer the
         * other [registerType] method that accepts a type name since a non-standard type's oid
         * might vary between database instances.
         *
         * Note, if you provide a type oid that belongs to a built-in type, this new type definition
         * will override the default and possibly cause the decoding and encoding of values to fail
         */
        suspend fun registerType(
            type: Int,
            dbType: DbType,
            hasArray: Boolean = true,
        ) {
            if (nonStandardTypesByOid.containsKey(type)) {
                logger.info("Replacing already registered custom type for type = {oid}", type)
            }
            nonStandardTypesByOid[type] = dbType to hasArray
        }

        /**
         * Register a new decoder with the specified [type] name. You should prefer this method over
         * the other [registerType] method since it allows the connection cache the oid on
         * startup and always match your database instance.
         *
         * Note, if you provide a type oid that belongs to a built-in type, this new type definition
         * will override the default and possibly cause the decoding and encoding of values to fail
         */
        suspend fun registerType(
            type: String,
            dbType: DbType,
            hasArray: Boolean = true,
        ) {
            if (nonStandardTypesByName.containsKey(type)) {
                logger.info("Replacing already registered custom type for type = {name}", type)
            }
            nonStandardTypesByName[type] = dbType to hasArray
        }

        @PublishedApi
        internal fun registerEnumType(
            dbType: DbType,
            type: String,
            hasArray: Boolean = true,
        ) {
            enumTypes[type] = dbType to hasArray
        }

        inline fun <reified E : Enum<E>> registerEnumType(
            type: String,
            hasArray: Boolean = true,
        ) = registerEnumType(enumDbType<E>(), type, hasArray)
    }
}
