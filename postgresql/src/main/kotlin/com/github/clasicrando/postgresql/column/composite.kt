package com.github.clasicrando.postgresql.column

import com.github.clasicrando.common.column.columnDecodeError
import com.github.clasicrando.postgresql.statement.PgArguments
import com.github.clasicrando.postgresql.type.PgCompositeLiteralParser
import kotlin.reflect.KClass
import kotlin.reflect.KType
import kotlin.reflect.full.memberProperties
import kotlin.reflect.full.primaryConstructor
import kotlin.reflect.typeOf

@PublishedApi
internal inline fun <reified T : Any> compositeTypeEncoder(name: String, typeRegistry: PgTypeRegistry): PgTypeEncoder<T> {
    require(T::class.isData) { "Only data classes are available as composites" }
    return PgCompositeTypeEncoder(typeRegistry, T::class, typeOf<T>(), PgType.ByName(name))
}

@PublishedApi
internal class PgCompositeTypeEncoder<T : Any>(
    private val typeRegistry: PgTypeRegistry,
    cls : KClass<T>,
    override val encodeType: KType,
    override var pgType: PgType
) : PgTypeEncoder<T> {
    private val propertyNames = cls.primaryConstructor
        ?.parameters
        ?.map { it.name!! }
        ?: error("Composite type must have primary constructor")
    private val properties = cls.memberProperties.filter { it.name in propertyNames }
        .map { it to typeRegistry.kindOf(it.returnType) }

    override fun encode(value: T, buffer: PgArguments) {
        buffer.writeInt(properties.size)
        for ((property, propertyTypeOid) in properties) {
            buffer.writeInt(propertyTypeOid.oidOrUnknown())
            typeRegistry.encode(property.get(value), buffer)
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

    private fun decodeAsBinary(value: PgValue.Binary): T {
        val length = value.bytes.readInt()
        val attributes = Array(length) { i ->
            val typeOid = innerTypes.getOrNull(i) ?: columnDecodeError(type, value.typeData)
            val fieldDescription = dummyTypedFieldDescription(typeOid.oidOrUnknown())
            val innerValue = PgValue.Binary(value.bytes, fieldDescription)
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
