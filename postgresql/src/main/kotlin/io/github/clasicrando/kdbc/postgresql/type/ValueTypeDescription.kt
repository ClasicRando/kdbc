package io.github.clasicrando.kdbc.postgresql.type

import io.github.clasicrando.kdbc.core.buffer.ByteWriteBuffer
import io.github.clasicrando.kdbc.postgresql.column.PgValue
import io.github.clasicrando.kdbc.postgresql.exceptions.PgException
import kotlin.reflect.KClass
import kotlin.reflect.KProperty1
import kotlin.reflect.KType
import kotlin.reflect.full.declaredMemberProperties
import kotlin.reflect.full.primaryConstructor
import kotlin.reflect.full.valueParameters

@Suppress("UNCHECKED_CAST")
internal class ValueTypeDescription<T : Any, I : Any>(
    valueClass: KClass<T>,
    valueType: KType,
    private val innerTypeDescription: PgTypeDescription<I>,
) : PgTypeDescription<T>(dbType = innerTypeDescription.dbType, kType = valueType) {
    init {
        require(valueClass.isValue) {
            "Type must be a value type to create wrapper type description"
        }
    }

    private val valueClassConstructor = valueClass.primaryConstructor!!
    private val innerTypeProperty: KProperty1<T, I>

    init {
        val innerValuePropertyName = valueClassConstructor.valueParameters.first().name!!
        innerTypeProperty =
            valueClass.declaredMemberProperties.first { it.name == innerValuePropertyName }
                as KProperty1<T, I>
    }

    override fun isCompatible(dbType: PgType): Boolean = innerTypeDescription.isCompatible(dbType)

    override fun encode(value: T, buffer: ByteWriteBuffer) {
        innerTypeDescription.encode(innerTypeProperty.get(value), buffer)
    }

    override fun decodeBytes(value: PgValue.Binary): T =
        try {
            valueClassConstructor.call(innerTypeDescription.decodeBytes(value))
        } catch (ex: IllegalArgumentException) {
            throw PgException(message = "Could not construct value class", ex = ex)
        }

    override fun decodeText(value: PgValue.Text): T =
        try {
            valueClassConstructor.call(innerTypeDescription.decodeText(value))
        } catch (ex: IllegalArgumentException) {
            throw PgException(message = "Could not construct value class", ex = ex)
        }
}
