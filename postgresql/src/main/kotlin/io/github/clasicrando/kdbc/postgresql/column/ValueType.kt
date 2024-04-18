package io.github.clasicrando.kdbc.postgresql.column

import kotlin.reflect.KTypeProjection
import kotlin.reflect.full.createType
import kotlin.reflect.full.memberProperties
import kotlin.reflect.full.primaryConstructor
import kotlin.reflect.full.valueParameters
import kotlin.reflect.full.withNullability

@PublishedApi
internal inline fun <reified T : Any> valueTypeEncoder(
    typeRegistry: PgTypeRegistry,
): Pair<PgTypeEncoder<T>, PgTypeEncoder<List<T?>>> {
    val cls = T::class
    require(cls.isValue) { "Attempted to register a non-value class as a wrapper value type" }
    val innerPropertyName = cls.primaryConstructor!!.valueParameters[0].name!!
    val property = cls.memberProperties.first { it.name == innerPropertyName }
    val innerKType = property.returnType

    val innerType = typeRegistry.kindOfInternal(property.returnType)
    val typeEncoder = PgTypeEncoder<T>(pgType = innerType) { value, buffer ->
        val innerPropertyValue = property.get(value)
        if (innerPropertyValue == null) {
            buffer.writeInt(-1)
            return@PgTypeEncoder
        }
        typeRegistry.encode(innerPropertyValue, buffer)
    }

    val innerTypeProjection = KTypeProjection.invariant(innerKType.withNullability(true))
    val listType = List::class.createType(arguments = listOf(innerTypeProjection))
    val arrayType = typeRegistry.kindOfInternal(listType)
    val arrayTypeEncoder = PgTypeEncoder<List<T?>>(pgType = arrayType) { value, buffer ->
        val simpleList = value.map { item ->
            item?.let { property.get(it) }
        }
        typeRegistry.encode(simpleList, buffer)
    }
    return typeEncoder to arrayTypeEncoder
}
