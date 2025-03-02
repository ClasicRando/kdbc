package io.github.clasicrando.kdbc.mysql.statement

import io.github.clasicrando.kdbc.core.query.QueryParameter
import io.github.clasicrando.kdbc.mysql.type.BigDecimalTypeDescription
import io.github.clasicrando.kdbc.mysql.type.BooleanTypeDescription
import io.github.clasicrando.kdbc.mysql.type.ByteArrayTypeDescription
import io.github.clasicrando.kdbc.mysql.type.DoubleTypeDescription
import io.github.clasicrando.kdbc.mysql.type.FloatTypeDescription
import io.github.clasicrando.kdbc.mysql.type.IntegerTypeDescription
import io.github.clasicrando.kdbc.mysql.type.LocalDateTimeTypeDescription
import io.github.clasicrando.kdbc.mysql.type.LocalDateTypeDescription
import io.github.clasicrando.kdbc.mysql.type.LocalTimeTypeDescription
import io.github.clasicrando.kdbc.mysql.type.LongTypeDescription
import io.github.clasicrando.kdbc.mysql.type.MySqlTypeCache
import io.github.clasicrando.kdbc.mysql.type.MySqlTypeDescription
import io.github.clasicrando.kdbc.mysql.type.ShortTypeDescription
import io.github.clasicrando.kdbc.mysql.type.StringTypeDescription
import io.github.clasicrando.kdbc.mysql.type.TinyIntTypeDescription
import kotlin.reflect.KType

/**
 * MySQL prepared statement parameter holding the [QueryParameter] and the associated
 * [MySqlTypeDescription] for encoding.
 */
internal class MySqlArgument
private constructor(val value: Any?, val typeDescription: MySqlTypeDescription<Any>) {
    companion object {
        fun of(parameter: QueryParameter, typeCache: MySqlTypeCache): MySqlArgument {
            return of(parameter.value, parameter.parameterType, typeCache)
        }

        fun of(value: Any?, kType: KType, typeCache: MySqlTypeCache): MySqlArgument {
            return MySqlArgument(
                value,
                when (value) {
                    is java.math.BigDecimal -> BigDecimalTypeDescription
                    is Boolean -> BooleanTypeDescription
                    is Byte -> TinyIntTypeDescription
                    is ByteArray -> ByteArrayTypeDescription
                    is Double -> DoubleTypeDescription
                    is Float -> FloatTypeDescription
                    is java.time.Instant -> typeCache.instantTypeDescription
                    is Int -> IntegerTypeDescription
                    is java.time.LocalDate -> LocalDateTypeDescription
                    is java.time.LocalDateTime -> LocalDateTimeTypeDescription
                    is java.time.LocalTime -> LocalTimeTypeDescription
                    is Long -> LongTypeDescription
                    is java.time.OffsetDateTime -> typeCache.offsetDateTimeTypeDescription
                    is Short -> ShortTypeDescription
                    is String -> StringTypeDescription
                    else -> typeCache.getTypeDescription<Any>(kType)
                } as MySqlTypeDescription<Any>,
            )
        }
    }
}
