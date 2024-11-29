package io.github.clasicrando.kdbc.mysql.type

import io.github.clasicrando.kdbc.core.atomic.AtomicMutableMap
import io.github.clasicrando.kdbc.core.exceptions.KdbcException
import kotlin.reflect.KType

internal class MySqlTypeCache {
    private val typeDescriptions: MutableMap<KType, MySqlTypeDescription<*>> =
        AtomicMutableMap(baseTypes)

    /**
     * Return the custom type description for the provided [kType]
     *
     * @throws KdbcException if the [kType] cannot be found in the lookup table
     */
    @Suppress("UNCHECKED_CAST")
    fun <T : Any> getTypeDescription(kType: KType): MySqlTypeDescription<T> {
        return typeDescriptions[kType] as? MySqlTypeDescription<T>
            ?: throw KdbcException("No type description for $kType")
    }

    /** Add a custom [typeDescription] to the lookup tables */
    internal fun <T : Any> addTypeDescription(typeDescription: MySqlTypeDescription<T>) {
        typeDescriptions[typeDescription.kType] = typeDescription
    }

    companion object {
        private val baseTypes: Map<KType, MySqlTypeDescription<*>> = listOf(
            BooleanTypeDescription,
            TinyIntTypeDescription,
            ShortTypeDescription,
            IntegerTypeDescription,
            LongTypeDescription,
            FloatTypeDescription,
            DoubleTypeDescription,
            ByteArrayTypeDescription,
            DurationTypeDescription,
            LocalTimeTypeDescription,
            LocalDateTypeDescription,
            LocalDateTimeTypeDescription,
            OffsetDateTimeTypeDescription,
            InstantTypeDescription,
            UuidTypeDescription,
            JUUIDTypeDescription,
            StringTypeDescription,
            BigDecimalTypeDescription,
            JsonTypeDescription,
            JsonTextTypeDescription,
            JsonBytesTypeDescription,
        ).associateBy { it.kType }
    }
}
