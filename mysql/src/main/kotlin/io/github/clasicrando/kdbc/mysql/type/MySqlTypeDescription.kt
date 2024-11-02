package io.github.clasicrando.kdbc.mysql.type

import io.github.clasicrando.kdbc.core.type.DbType
import io.github.clasicrando.kdbc.mysql.result.MySqlValue
import kotlin.reflect.KType

abstract class MySqlTypeDescription<T : Any>(
    /**
     * [MySqlType] that is referenced for this type description as the serialization input and
     * deserialization output
     */
    final override val dbType: MySqlType,
    /** Kotlin type of [T] that is recognized by this type description */
    final override val kType: KType,
) : DbType<T, MySqlValue, MySqlType>
