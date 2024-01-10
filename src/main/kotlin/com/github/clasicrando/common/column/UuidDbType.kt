package com.github.clasicrando.common.column

import kotlinx.uuid.UUID
import kotlin.reflect.KClass
/**
 * [DbType] for database types that represent
 * [UUID](https://en.wikipedia.org/wiki/Universally_unique_identifier) values. Values as encoded
 * and decoded as strings. Although it is easy to convert the decoded value to a java uuid, the
 * actual type returned is a kotlinx variant for multi-platform support.
 */
object UuidDbType : DbType {
    override fun decode(type: ColumnInfo, value: String): Any = UUID(value)

    override val encodeType: KClass<*> = UUID::class
}
