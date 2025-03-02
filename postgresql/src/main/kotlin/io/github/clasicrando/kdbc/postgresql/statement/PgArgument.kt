package io.github.clasicrando.kdbc.postgresql.statement

import io.github.clasicrando.kdbc.core.query.QueryParameter
import io.github.clasicrando.kdbc.postgresql.type.BigDecimalTypeDescription
import io.github.clasicrando.kdbc.postgresql.type.BigIntTypeDescription
import io.github.clasicrando.kdbc.postgresql.type.BoolTypeDescription
import io.github.clasicrando.kdbc.postgresql.type.ByteaTypeDescription
import io.github.clasicrando.kdbc.postgresql.type.CharTypeDescription
import io.github.clasicrando.kdbc.postgresql.type.DoublePrecisionTypeDescription
import io.github.clasicrando.kdbc.postgresql.type.IntTypeDescription
import io.github.clasicrando.kdbc.postgresql.type.LocalDateTimeTypeDescription
import io.github.clasicrando.kdbc.postgresql.type.LocalDateTypeDescription
import io.github.clasicrando.kdbc.postgresql.type.LocalTimeTypeDescription
import io.github.clasicrando.kdbc.postgresql.type.PgTypeCache
import io.github.clasicrando.kdbc.postgresql.type.PgTypeDescription
import io.github.clasicrando.kdbc.postgresql.type.RealTypeDescription
import io.github.clasicrando.kdbc.postgresql.type.SmallIntTypeDescription
import io.github.clasicrando.kdbc.postgresql.type.VarcharTypeDescription
import kotlin.reflect.KType

/** Argument for Postgres queries containing the [parameter] value and it's [PgTypeDescription] */
internal class PgArgument
private constructor(val parameter: Any?, val pgTypeDescription: PgTypeDescription<Any>) {
    companion object {
        fun of(parameter: QueryParameter, typeCache: PgTypeCache): PgArgument {
            return of(parameter.value, parameter.parameterType, typeCache)
        }

        fun of(value: Any?, kType: KType, typeCache: PgTypeCache): PgArgument {
            return PgArgument(
                value,
                when (value) {
                    is java.math.BigDecimal -> BigDecimalTypeDescription
                    is Boolean -> BoolTypeDescription
                    is Byte -> CharTypeDescription
                    is ByteArray -> ByteaTypeDescription
                    is Double -> DoublePrecisionTypeDescription
                    is Float -> RealTypeDescription
                    is java.time.Instant -> typeCache.instantDescription
                    is Int -> IntTypeDescription
                    is java.time.LocalDate -> LocalDateTypeDescription
                    is java.time.LocalDateTime -> LocalDateTimeTypeDescription
                    is java.time.LocalTime -> LocalTimeTypeDescription
                    is Long -> BigIntTypeDescription
                    is java.time.OffsetDateTime -> typeCache.offsetDateTimeDescription
                    is Short -> SmallIntTypeDescription
                    is String -> VarcharTypeDescription
                    else -> typeCache.getTypeDescription<Any>(kType)
                } as PgTypeDescription<Any>,
            )
        }
    }
}
