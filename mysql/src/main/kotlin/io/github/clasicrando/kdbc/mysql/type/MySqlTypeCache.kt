package io.github.clasicrando.kdbc.mysql.type

import io.github.clasicrando.kdbc.core.atomic.AtomicMutableMap
import io.github.clasicrando.kdbc.core.exceptions.KdbcException
import io.github.clasicrando.kdbc.mysql.exceptions.MySqlException
import java.time.ZoneOffset
import kotlin.reflect.KType

/**
 * Type cache for [MySqlTypeDescription]s. Requires a [ZoneOffset] to notify the offset that should
 * be applied to timezone aware types when decoding.
 */
internal class MySqlTypeCache(zoneOffset: ZoneOffset) {
    private val typeDescriptions: MutableMap<KType, MySqlTypeDescription<*>> =
        AtomicMutableMap(getBaseTypes(zoneOffset = zoneOffset))

    /**
     * Return the custom type description for the provided [kType]
     *
     * @throws KdbcException if the [kType] cannot be found in the lookup table
     */
    @Suppress("UNCHECKED_CAST")
    fun <T : Any> getTypeDescription(kType: KType): MySqlTypeDescription<T> {
        return typeDescriptions[kType] as? MySqlTypeDescription<T>
            ?: throw MySqlException("No type description for $kType")
    }

    /** Add a custom [typeDescription] to the lookup tables */
    internal fun <T : Any> addTypeDescription(typeDescription: MySqlTypeDescription<T>) {
        typeDescriptions[typeDescription.kType] = typeDescription
    }

    companion object {
        private fun getBaseTypes(zoneOffset: ZoneOffset): Map<KType, MySqlTypeDescription<*>> {
            return listOf(
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
                    OffsetDateTimeTypeDescription(zoneOffset = zoneOffset),
                    InstantTypeDescription(zoneOffset = zoneOffset),
                    UuidTypeDescription,
                    JUUIDTypeDescription,
                    StringTypeDescription,
                    BigDecimalTypeDescription,
                    JsonTypeDescription,
                    JsonTextTypeDescription,
                    JsonBytesTypeDescription,
                )
                .associateBy { it.kType }
        }
    }
}
