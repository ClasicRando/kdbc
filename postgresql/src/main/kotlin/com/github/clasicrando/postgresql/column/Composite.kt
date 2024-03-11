package com.github.clasicrando.postgresql.column

import com.github.clasicrando.common.buffer.ByteWriteBuffer
import com.github.clasicrando.common.column.columnDecodeError
import com.github.clasicrando.common.column.ColumnDecodeError
import com.github.clasicrando.postgresql.type.PgCompositeLiteralParser
import kotlin.reflect.KClass
import kotlin.reflect.KFunction
import kotlin.reflect.KType
import kotlin.reflect.full.memberProperties
import kotlin.reflect.full.primaryConstructor
import kotlin.reflect.typeOf

/**
 * Function for creating new [PgCompositeTypeEncoder] instances. This accepts the type name as
 * seen in the database and the [typeRegistry] that will be used to resolve encoders for each
 * attribute of the composite. Composite types can only be represented as data classes so any other
 * type provided as [T] will throw an [IllegalArgumentException].
 *
 * @throws IllegalArgumentException if the type [T] is not a data class
 */
@PublishedApi
internal inline fun <reified T : Any> compositeTypeEncoder(
    name: String,
    typeRegistry: PgTypeRegistry,
): PgTypeEncoder<T> {
    require(T::class.isData) { "Only data classes are available as composites" }
    return PgCompositeTypeEncoder(
        typeRegistry = typeRegistry,
        cls = T::class,
        encodeTypes = listOf(typeOf<T>()),
        pgType = PgType.ByName(name),
    )
}

/**
 * Implementation of a [PgTypeEncoder] for composite types. This is done by using reflection to
 * derive properties in the data class' constructor, finding the [PgType] of each property and
 * encoding instances of [T] by reading each property (using reflection) for encoding into the
 * argument buffer.
 *
 * To allow for encoding any type of property found in a composite, a reference to the
 * [typeRegistry] containing this composite encoder is kept so encoding can be done on the fly
 * as needed.
 *
 * @throws IllegalArgumentException if the primary constructor of the type is null (this should
 * never happen)
 */
@PublishedApi
internal class PgCompositeTypeEncoder<T : Any>(
    private val typeRegistry: PgTypeRegistry,
    cls : KClass<T>,
    override val encodeTypes: List<KType>,
    override var pgType: PgType
) : PgTypeEncoder<T> {
    private val propertyNames: List<String>
    init {
        val primaryConstructor = cls.primaryConstructor
        requireNotNull(primaryConstructor) { "Composite type must have primary constructor" }
        propertyNames = primaryConstructor.parameters.map { it.name!! }
    }
    private val properties = cls.memberProperties
        .filter { it.name in propertyNames }
        .map { it to typeRegistry.kindOf(it.returnType) }

    /**
     * Encode [value] into the arguments [buffer]. This writes:
     * 1. The number of properties/attributes in the type
     * 2. For each property:
     *     1. The property's type Oid
     *     2. The property value encoded and length prefixed (length is -1 if the value is null)
     *
     * [pg source code](https://github.com/postgres/postgres/blob/874d817baa160ca7e68bee6ccc9fc1848c56e750/src/backend/utils/adt/rowtypes.c#L481)
     */
    override fun encode(value: T, buffer: ByteWriteBuffer) {
        buffer.writeInt(properties.size)
        for ((property, propertyTypeOid) in properties) {
            buffer.writeInt(propertyTypeOid.oidOrUnknown())
            val propertyValue = property.get(value)
            if (propertyValue == null) {
                buffer.writeInt(-1)
                continue
            }
            buffer.writeLengthPrefixed {
                typeRegistry.encode(propertyValue, this)
            }
        }
    }
}

/**
 * Function for creating new [PgCompositeTypeDecoder] instances. This accepts the [typeRegistry]
 * that will be used to resolve decoders for each attribute of the composite. Composite types can
 * only be represented as data classes so any other type provided as [T] will throw an
 * [IllegalArgumentException].
 *
 * @throws IllegalArgumentException if the type [T] is not a data class
 */
@PublishedApi
internal inline fun <reified T : Any> compositeTypeDecoder(
    typeRegistry: PgTypeRegistry,
): PgTypeDecoder<T> {
    require(T::class.isData) { "Only data classes are available as composites" }
    return PgCompositeTypeDecoder(typeRegistry, T::class, typeOf<T>())
}

/**
 * Implementation of a [PgTypeDecoder] for composite types. This is done by using reflection to
 * derive properties in the data class' constructor, finding the [PgType] of each property and
 * decoding instances of [T] by reading the row buffer and calling the primary constructor of the
 * class using reflection.
 *
 * To allow for decoding any type of property found in a composite, a reference to the
 * [typeRegistry] containing this composite decoder is kept so decoding can be done on the fly
 * as needed.
 *
 * @throws IllegalArgumentException if the primary constructor of the type is null (this should
 * never happen)
 */
@PublishedApi
internal class PgCompositeTypeDecoder<T : Any>(
    private val typeRegistry: PgTypeRegistry,
    cls: KClass<T>,
    private val type: KType,
) : PgTypeDecoder<T> {
    private val primaryConstructor: KFunction<T>
    init {
        val constructor = cls.primaryConstructor
        requireNotNull(constructor) { "Composite type must have primary constructor" }
        primaryConstructor = constructor
    }
    private val innerTypes = primaryConstructor.parameters
        .map { typeRegistry.kindOf(it.type) }

    /**
     * Use the [PgCompositeLiteralParser] to parse each property in order, map each [String] into
     * the desired type using the [typeRegistry] and collect that into an array that is passed to
     * the [primaryConstructor] of the composite type.
     *
     * [pg source code](https://github.com/postgres/postgres/blob/874d817baa160ca7e68bee6ccc9fc1848c56e750/src/backend/utils/adt/rowtypes.c#L330)
     *
     * @throws ColumnDecodeError if the call to the [primaryConstructor] fails or any decoding of
     * property values fails
     */
    private fun decodeAsStr(value: PgValue.Text): T {
        val attributes = PgCompositeLiteralParser.parse(value.text)
            .mapIndexed { i, str ->
                if (str == null) {
                    return@mapIndexed null
                }
                innerTypes.getOrNull(i)
                    ?.let {
                        val typeData = dummyTypedFieldDescription(it.oidOrUnknown())
                        typeRegistry.decode(PgValue.Text(str, typeData))
                    }
                    ?: columnDecodeError(type, value.typeData)
            }
            .toList()
            .toTypedArray()
        return try {
            primaryConstructor.call(*attributes)
        } catch (ex: Throwable) {
            columnDecodeError(type, value.typeData)
        }
    }

    /**
     * Decode the binary [value] as an [Array] of values that are used in a call to the
     * [primaryConstructor] of the type [T]. Steps are as follows:
     *
     * 1. Read the first [Int] of the buffer as the number of properties remaining in the buffer.
     * 2. Construct an [Array] with the size already fetched where each element created as:
     *     1. Read the next int as the element's Oid
     *     2. Use that Oid to create a column description
     *     3. Read the next int as the number of upcoming bytes for the composite attribute
     *     4. Construct a PgValue to pass to the type registry for decoding the composite attribute
     *     5. Set that decoded value as the array element
     * 3. With all the elements obtained, call the [primaryConstructor] to get a new instance of
     * [T].
     *
     * [pg source code](https://github.com/postgres/postgres/blob/874d817baa160ca7e68bee6ccc9fc1848c56e750/src/backend/utils/adt/rowtypes.c#L688)
     *
     * @throws ColumnDecodeError if the call to the [primaryConstructor] fails or any decoding of
     * property values fails
     */
    private fun decodeAsBinary(value: PgValue.Binary): T {
        val length = value.bytes.readInt()
        val attributes = Array(length) {
            val typeOid = PgType.fromOid(value.bytes.readInt())
            val fieldDescription = dummyTypedFieldDescription(typeOid.oidOrUnknown())
            val attributeLength = value.bytes.readInt()
            val slice = value.bytes.slice(attributeLength)
            value.bytes.skip(attributeLength)
            val innerValue = PgValue.Binary(slice, fieldDescription)
            typeRegistry.decode(innerValue)
        }
        return try {
            primaryConstructor.call(*attributes)
        } catch (ex: Throwable) {
            columnDecodeError(type, value.typeData)
        }
    }

    override fun decode(value: PgValue): T {
        return when (value) {
            is PgValue.Binary -> decodeAsBinary(value)
            is PgValue.Text -> decodeAsStr(value)
        }
    }
}
