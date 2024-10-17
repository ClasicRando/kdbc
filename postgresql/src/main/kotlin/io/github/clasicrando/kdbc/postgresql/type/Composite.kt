package io.github.clasicrando.kdbc.postgresql.type

import io.github.clasicrando.kdbc.core.annotations.Rename
import io.github.clasicrando.kdbc.core.buffer.ByteWriteBuffer
import io.github.clasicrando.kdbc.core.column.columnDecodeError
import io.github.clasicrando.kdbc.core.query.RowParser
import io.github.clasicrando.kdbc.core.result.DataRow
import io.github.clasicrando.kdbc.postgresql.column.PgColumnDescription
import io.github.clasicrando.kdbc.postgresql.column.PgValue
import io.github.clasicrando.kdbc.postgresql.result.PgDataRow
import io.github.clasicrando.kdbc.postgresql.statement.PgEncodeBuffer
import kotlin.reflect.KClass
import kotlin.reflect.KType
import kotlin.reflect.full.createType
import kotlin.reflect.full.memberProperties
import kotlin.reflect.full.primaryConstructor

/**
 * Base requirements for a type description of a Postgresql composite type. The type must define how
 * to parse a [DataRow] into the type and also how to extract values that make up the composite
 * type.
 */
interface CompositeTypeDefinition<T : Any> : RowParser<T> {
    /**
     * Custom behaviour to return the composite instance's attribute values paired with the values
     * type.
     */
    fun extractValues(value: T): List<Pair<Any?, KType>>
}

/**
 * Implementation of a [PgTypeDescription] for composite types. This requires the composite type's
 * name, the column mapping and a strategy to decode the composite's attributes from a [PgDataRow].
 */
internal abstract class BaseCompositeTypeDescription<T : Any>(
    typeOid: Int,
    protected val columnMapping: List<PgColumnDescription>,
    protected val customTypeDescriptionCache: PgTypeCache,
    kType: KType,
) : CompositeTypeDefinition<T>, PgTypeDescription<T>(
    dbType = PgType.ByOid(oid = typeOid),
    kType = kType,
) {
    /**
     * To encode the values into the buffer, first fetch all the composite type instance's
     * attribute values (with value's [KType] as well) then write:
     *
     * 1. The number of attributes for the composite type
     * 2. For each attribute
     *     - the OID of the attribute's type
     *     - the length of the attribute's value in bytes
     *     - the attribute value in bytes
     *
     * @throws IllegalStateException if the number of
     */
    final override fun encode(value: T, buffer: ByteWriteBuffer) {
        val values = extractValues(value)
        check(values.size == columnMapping.size) {
            "Values found for composite class instance does not match the expected number. " +
                    "Expected ${columnMapping.size}, found ${values.size}"
        }
        val encodeBuffer = PgEncodeBuffer(
            parameterTypeOids = columnMapping.map { it.pgType.oid },
            typeCache = customTypeDescriptionCache,
        )
        buffer.writeInt(columnMapping.size)
        for (i in columnMapping.indices) {
            val column = columnMapping[i]
            buffer.writeInt(column.pgType.oid)
            val (attribute, kType) = values[i]
            encodeBuffer.encodeValue(attribute, kType)
            buffer.copyFrom(encodeBuffer.innerBuffer)
        }
    }

    /**
     * Pipe the [attributes] to the [fromRow] method implemented by all composite type definitions.
     * If any exception is thrown, it will be converted to a
     * [io.github.clasicrando.kdbc.core.column.ColumnDecodeError] with the original exceptions
     * message included.
     *
     * @throws io.github.clasicrando.kdbc.core.column.ColumnDecodeError if the [fromRow] method
     * throws an exception
     */
    private fun decodeAsDataRow(attributes: PgDataRow, typeData: PgColumnDescription): T {
        return try {
            fromRow(attributes)
        } catch (ex: Exception) {
            columnDecodeError(
                kType = kType,
                type = typeData,
                reason = "Could not construct composite type",
                cause = ex,
            )
        }
    }

    /**
     * Decode the binary [value] as an [Array] of [PgValue]s that are used in a call to the
     * [decodeAsDataRow] method. Steps are as follows:
     *
     * 1. Read the first [Int] of the buffer as the number of properties remaining in the buffer.
     * 2. Construct an [Array] with the size already fetched where each element created as:
     *     1. Read the next int as the element's OID
     *     2. Use that OID to create a column description
     *     3. Read the next int as the number of upcoming bytes for the composite attribute
     *     4. Construct a PgValue to pass to the type registry for decoding the composite attribute
     *     5. Set that decoded value as the array element
     * 3. With all the elements obtained, call [decodeAsDataRow] to allow the custom parsing to
     * occur
     *
     * [pg source code](https://github.com/postgres/postgres/blob/874d817baa160ca7e68bee6ccc9fc1848c56e750/src/backend/utils/adt/rowtypes.c#L688)
     *
     * @throws io.github.clasicrando.kdbc.core.column.ColumnDecodeError if parsing in [fromRow]
     * fails
     */
    final override fun decodeBytes(value: PgValue.Binary): T {
        val length = value.bytes.readInt()
        val attributes = Array<PgValue?>(length) {
            value.bytes.readInt()

            val attributeLength = value.bytes.readInt()
            if (attributeLength == -1) {
                return@Array null
            }
            val slice = value.bytes.slice(attributeLength)
            PgValue.Binary(slice, columnMapping[it])
        }
        val dataRow = PgDataRow(
            rowBuffer = value.bytes,
            pgValues = attributes,
            columnMapping = columnMapping,
            typeCache = customTypeDescriptionCache,
        )
        return decodeAsDataRow(dataRow, value.typeData)
    }

    /**
     * Use the [PgCompositeLiteralParser] to parse each property in order, map each [String] into
     * a [PgValue] and collect that into a [PgDataRow] for parsing using the [decodeAsDataRow]
     * method.
     *
     * [pg source code](https://github.com/postgres/postgres/blob/874d817baa160ca7e68bee6ccc9fc1848c56e750/src/backend/utils/adt/rowtypes.c#L330)
     *
     * @throws io.github.clasicrando.kdbc.core.column.ColumnDecodeError if parsing in [fromRow]
     * fails
     */
    final override fun decodeText(value: PgValue.Text): T {
        val attributes = PgCompositeLiteralParser
            .parse(value.text)
            .withIndex()
            .map { (i, value) ->
                val text = value ?: return@map null
                PgValue.Text(text, columnMapping[i])
            }
            .toList()
            .toTypedArray<PgValue?>()
        val dataRow = PgDataRow(
            rowBuffer = null,
            pgValues = attributes,
            columnMapping = columnMapping,
            typeCache = customTypeDescriptionCache,
        )
        return decodeAsDataRow(dataRow, value.typeData)
    }
}

/**
 * Implementation of a [BaseCompositeTypeDescription] that uses reflection to [extractValues] from
 * instances of [T] and create new instances of [T] by calling the primary constructor. This class
 * only works for data class definitions and will throw an [IllegalArgumentException] during the
 * constructor call if that is not the case. It also requires that the number of constructor
 * parameters matches the supplied columns mapping. Other requirements (such as the composite
 * type's attributes matching the expected data class properties) are up to the class definer.
 */
internal class ReflectionCompositeTypeDescription<T : Any>(
    typeOid: Int,
    columnMapping: List<PgColumnDescription>,
    customTypeDescriptionCache: PgTypeCache,
    cls: KClass<T>,
) : BaseCompositeTypeDescription<T>(
    typeOid = typeOid,
    columnMapping = columnMapping,
    customTypeDescriptionCache = customTypeDescriptionCache,
    kType = cls.createType(),
) {
    init {
        require(cls.isData) { "Only data classes are allowed to represent composite types" }
    }
    private val primaryConstructor = cls.primaryConstructor!!
    private val constructorParameterNames = primaryConstructor.parameters.map { it.name!! }
    private val properties = constructorParameterNames.map { param ->
        cls.memberProperties.first { it.name == param }
    }
    private val finalParameterNames = primaryConstructor.parameters
        .map { param ->
            val name = param.annotations
                .firstOrNull { it is Rename }
                ?.let { it as Rename }
                ?.value
                ?: param.name!!
            name to param.type
        }
    init {
        require(columnMapping.size == constructorParameterNames.size) {
            "Declared composite data class does not match the number of expected attributes"
        }
    }

    override fun extractValues(value: T): List<Pair<Any?, KType>> {
        return properties.map { it.call(value) to it.returnType }
    }

    override fun fromRow(row: DataRow): T {
        val args = Array(finalParameterNames.size) { i ->
            val (parameterName, parameterType) = finalParameterNames[i]
            row[parameterName, parameterType]
        }
        return primaryConstructor.call(*args)
    }
}

/** Implementation of an [ArrayTypeDescription] for composite types */
internal class CompositeArrayTypeDescription<T : Any>(
    pgType: PgType,
    innerType: BaseCompositeTypeDescription<T>,
    innerNullable: Boolean,
) : ArrayTypeDescription<T>(pgType = pgType, innerType = innerType, innerNullable = innerNullable)
