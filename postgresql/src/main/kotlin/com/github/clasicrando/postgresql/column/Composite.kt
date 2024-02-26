package com.github.clasicrando.postgresql.column

import com.github.clasicrando.common.buffer.ByteWriteBuffer
import com.github.clasicrando.common.column.columnDecodeError
import com.github.clasicrando.postgresql.type.PgCompositeLiteralParser
import kotlin.reflect.KClass
import kotlin.reflect.KType
import kotlin.reflect.full.memberProperties
import kotlin.reflect.full.primaryConstructor
import kotlin.reflect.typeOf

@PublishedApi
internal inline fun <reified T : Any> compositeTypeEncoder(name: String, typeRegistry: PgTypeRegistry): PgTypeEncoder<T> {
    require(T::class.isData) { "Only data classes are available as composites" }
    return PgCompositeTypeEncoder(
        typeRegistry = typeRegistry,
        cls = T::class,
        encodeTypes = listOf(typeOf<T>()),
        pgType = PgType.ByName(name),
    )
}

@PublishedApi
internal class PgCompositeTypeEncoder<T : Any>(
    private val typeRegistry: PgTypeRegistry,
    cls : KClass<T>,
    override val encodeTypes: List<KType>,
    override var pgType: PgType
) : PgTypeEncoder<T> {
    private val propertyNames = cls.primaryConstructor
        ?.parameters
        ?.map { it.name!! }
        ?: error("Composite type must have primary constructor")
    private val properties = cls.memberProperties.filter { it.name in propertyNames }
        .map { it to typeRegistry.kindOf(it.returnType) }

    // https://github.com/postgres/postgres/blob/874d817baa160ca7e68bee6ccc9fc1848c56e750/src/backend/utils/adt/rowtypes.c#L481
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

@PublishedApi
internal inline fun <reified T : Any> compositeTypeDecoder(
    typeRegistry: PgTypeRegistry,
): PgTypeDecoder<T> {
    require(T::class.isData) { "Only data classes are available as composites" }
    return PgCompositeTypeDecoder(typeRegistry, T::class, typeOf<T>())
}

@PublishedApi
internal class PgCompositeTypeDecoder<T : Any>(
    private val typeRegistry: PgTypeRegistry,
    cls: KClass<T>,
    private val type: KType,
) : PgTypeDecoder<T> {
    private val primaryConstructor = cls.primaryConstructor
        ?: error("Composite type must have primary constructor")
    private val innerTypes = primaryConstructor.parameters
        .map { typeRegistry.kindOf(it.type) }

    // https://github.com/postgres/postgres/blob/874d817baa160ca7e68bee6ccc9fc1848c56e750/src/backend/utils/adt/rowtypes.c#L330
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

    // https://github.com/postgres/postgres/blob/874d817baa160ca7e68bee6ccc9fc1848c56e750/src/backend/utils/adt/rowtypes.c#L688
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
